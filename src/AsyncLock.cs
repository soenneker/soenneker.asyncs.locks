using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Asyncs.Locks.Abstract;
using Soenneker.Atomics.ValueInts;

namespace Soenneker.Asyncs.Locks;

/// <inheritdoc cref="IAsyncLock"/>
public sealed class AsyncLock : IAsyncLock
{
    private const int _availableNoWaiters = 0;
    private const int _disposeBit = 1;
    private const int _lockValue = 2;

    /// <summary>
    /// State encoding:
    /// - bit0: dispose flag (0 = in use, 1 = disposed)
    /// - bits1..: acquire count * 2 (including current holder, so we can add/subtract 2 per acquire while preserving bit0)
    /// 
    /// This information is encoded in a single value so that locking or adding a waiter
    /// while checking if disposed can all happen in a single fetch-and-add (FAA) operation.
    /// </summary>
    private ValueAtomicInt _state;

    // Invariant: Queue always contains a spare waiter for the next contended lock access.
    private WaiterHandle _waiterQueueHead;
    private WaiterHandle _waiterQueueTail;

    // Used by DisposeAsync() to wait until the lock becomes free after Dispose() has been called.
    // Must only be accessed under _gate to avoid races.
    private readonly TaskCompletionSource _disposeWaiter = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Initialize queue with the spare waiter
    public AsyncLock() => _waiterQueueHead = _waiterQueueTail = Waiter.Rent();

    // Used in Lock methods to claim a waiter for contended lock access
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClaimWaiter(out WaiterHandle handle)
    {
        // Claim the current spare waiter and push a new one
        var newSpare = Waiter.Rent();
        handle = Interlocked.Exchange(ref _waiterQueueTail, newSpare);
        handle.Next = newSpare;
        
        if (handle.Process())
        {
            // If the waiter has already been completed (in Exit()), then it is safe
            // to assume the handle is at the head of the queue. It is also now our
            // responsibility to pop the queue. However, we might be racing with
            // Dispose() or Exit(), so only pop the queue if the head is our waiter.
            Interlocked.CompareExchange(ref _waiterQueueHead, newSpare, handle);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            // Disposal takes precedence over cancellation
            if ((_state.Value & _disposeBit) != 0)
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            
            return ValueTask.FromCanceled<Releaser>(cancellationToken);
        }
        
        var state = _state.Add(_lockValue);
        
        // Fast path: free -> held
        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));
        
        // Slow path: The lock is already held or the lock is disposed
        return LockSlow(state, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock()
    {
        // See notes in Lock(CancellationToken)
        var state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlowNoToken(state);
    }
    
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(int state, CancellationToken cancellationToken)
    {
        if ((state & _disposeBit) != 0)
        {
            // The only value to unannouncing our waiter is to make it more likely for the
            // remaining Exit() to take the fast path.
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }
        
        ClaimWaiter(out var handle);
        
        // Catch the case where disposal occurred between incrementing the
        // acquire count and claiming a waiter.
        if ((_state.Value & _disposeBit) != 0)
        {
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }
        
        return handle.NewValueTask(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken(int state)
    {
        // See notes in LockSlow(CancellationToken)
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }
        
        ClaimWaiter(out var handle);
        
        if ((_state.Value & _disposeBit) != 0)
        {
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return handle.NewValueTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryLock(out Releaser releaser)
    {
        if (_state.TrySet(_lockValue, _availableNoWaiters))
        {
            releaser = new Releaser(this);
            return true;
        }

        releaser = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Releaser LockSync(CancellationToken cancellationToken = default)
    {
        // See notes in Lock(CancellationToken)
        if (cancellationToken.IsCancellationRequested)
        {
            if ((_state.Value & _disposeBit) != 0)
                throw new ObjectDisposedException(nameof(AsyncLock));
            
            throw new OperationCanceledException(cancellationToken);
        }
        
        var state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new Releaser(this);
        
        return LockSyncSlow(state, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(int state, CancellationToken cancellationToken)
    {
        // See notes in LockSlow(CancellationToken)
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            throw new ObjectDisposedException(nameof(AsyncLock));
        }

        ClaimWaiter(out var handle);
        
        if ((_state.Value & _disposeBit) != 0)
        {
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return handle.NewValueTask(cancellationToken).AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        while (true)
        {
            // Fast path: Lock released with no announced waiters
            if (_state.Add(-_lockValue) is var state and < _lockValue)
            {
                if ((state & _disposeBit) != 0)
                {
                    _disposeWaiter.TrySetResult();
                }

                return;
            }

            // Slow path: waiters announced
            while (true)
            {
                var head = Volatile.Read(ref _waiterQueueHead);

                if ((_state.Value & _disposeBit) != 0)
                {
                    _disposeWaiter.TrySetResult();
                    return;
                }
                
                if (head.TryGrant(new Releaser(this)))
                {
                    if (head.Process())
                    {
                        // The waiter has already been claimed by a Lock() method, so it is our
                        // responsibility to pop the queue. However, we might be racing with
                        // Dispose() or the previous call to Exit(), so only pop the queue
                        // if the head is our waiter.
                        Interlocked.CompareExchange(ref _waiterQueueHead, head.Next!, head);
                    }
                    // else: The waiter has been announced, but not yet claimed by a Lock() method.
                    // In this case, it is okay to leave the waiter in the queue. There will
                    // not be another call to Exit() until the Lock() method completes and
                    // maintains the queue invariant.

                    return;
                }

                if (head.Next is not null)
                { 
                    // If we could not transfer the lock and the waiter is linked,
                    // then one of three things happened:
                    // - The waiter was canceled.
                    // - The lock was disposed.
                    // - The previous owner of the lock has not yet popped the queue.
                    // We will pop the queue and try to transfer the lock again. However, we might
                    // be racing with Dispose() or the previous call to Exit(), so only pop the
                    // queue if the head is our waiter.
                    Interlocked.CompareExchange(ref _waiterQueueHead, head.Next, head);

                    // Whether to break or continue determines whether the acquire count is decremented
                    // before trying again. If the handle was granted the lock, then we do not want
                    // to decrement again, or a waiter will be lost.
                    if (head.IsGranted)
                        continue;

                    break;
                }

                // If we could not transfer the lock and the waiter is not linked,
                // then it must have been disposed.
                _disposeWaiter.TrySetResult();
                return;
            }
        }
    }

    public void Dispose()
    {
        // Set the lock state to disposed with lock taken, regardless of whether the lock
        // is actually held. If the lock is free, then there is no more work to do. If
        // the lock is held, then the count of any waiters is cleared.
        var state = _state.Exchange(_disposeBit + _lockValue);
        
        if ((state & _disposeBit) != 0)
            return;

        // If we disposed a free lock, then complete the dispose task
        if (state == _availableNoWaiters)
        {
            _disposeWaiter.TrySetResult();
            return;
        }

        // Take the entire queue, leave behind the spare waiter to maintain the queue invariant.
        // It's ok if a Lock() method is racing to add a waiter, it will eventually check the
        // dispose bit and fault the waiter. At worst, any racing Lock() method might leave an
        // unused waiter, which will become eligible for garbage collection along with the lock.
        var tail = Volatile.Read(ref _waiterQueueTail);
        var handle = Interlocked.Exchange(ref _waiterQueueHead, tail);
        
        // Fault each waiter
        var ode = new ObjectDisposedException(nameof(AsyncLock));
        while (handle != tail)
        {
            handle.TrySetException(ode);
            handle = handle.Next;

            if (handle is null)
                break;
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return new ValueTask(_disposeWaiter.Task);
    }
}
