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
    private const int _lockBit = 1;
    private const int _disposeBit = 2;
    private const int _waitersValue = 4;

    /// <summary>
    /// State encoding:
    /// - bit0: held flag (0 = free, 1 = held)
    /// - bit1: dispose flag (0 = in use, 1 = disposed)
    /// - bits2..: announced waiter count * 4 (so we can add/subtract 4 per waiter while preserving bit0 and bit1)
    /// 
    /// This information is encoded in a single value so that checking if the lock is free,
    /// not disposed, and there are no waiters can all happen in a single CAS for the fast path.
    /// </summary>
    private ValueAtomicInt _state;

    // Faster to use than Lock currently in .NET 10
    // ReSharper disable once ChangeFieldTypeToSystemThreadingLock
    private readonly object _gate = new();

    // Invariant: Queue always contains a spare waiter for the next contended lock access.
    private WaiterHandle _waiterQueueHead;
    private WaiterHandle _waiterQueueTail;

    // Used by DisposeAsync() to wait until the lock becomes free after Dispose() has been called.
    // Must only be accessed under _gate to avoid races.
    private TaskCompletionSource? _disposeWaiter;

    public AsyncLock() => _waiterQueueHead = _waiterQueueTail = Waiter.Rent();

    // Used in Lock methods when handing out a waiter to a consumer.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClaimWaiter(out WaiterHandle handle)
    {
        // Claim the current spare waiter and push a new one
        handle = _waiterQueueTail;
        _waiterQueueTail = handle.Next = Waiter.Rent();

        if (handle.Processed)
        {
            // If the waiter has already been completed (in Exit()), then it is safe
            // to assume the handle is at the head of the queue.
            _waiterQueueHead = _waiterQueueHead.Next!;
            handle.Next = null;
        }
        else
        {
            handle.Processed = true;
        }
    }

    // Remove the head of the queue.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PopWaiter()
    {
        var handle = _waiterQueueHead;
        _waiterQueueHead = handle.Next!;
        handle.Next = null;
    }

    // Returns the head of the queue.
    // Used in Exit() when handing over control of the lock.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WaiterHandle NextWaiter()
    {
        var handle = _waiterQueueHead;

        // If the waiter has already been claimed, pop the queue.
        // Otherwise, leave the waiter in the queue. The active Lock method will claim
        // it and take responsibility for popping the queue.
        if (handle.Processed)
        {
            PopWaiter();
        }
        else
        {
            handle.Processed = true;
        }

        return handle;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock(CancellationToken cancellationToken)
    {
        if ((_state.Value & _disposeBit) != 0)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<Releaser>(cancellationToken);

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(_lockBit, _availableNoWaiters))
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock()
    {
        if ((_state.Value & _disposeBit) != 0)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(_lockBit, _availableNoWaiters))
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlowNoToken();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(CancellationToken cancellationToken)
    {
        // Ensure one of the following is true when the loop exits:
        // - The lock is acquired.
        // - The waiter count is incremented.
        //
        // This avoids the case where a waiter is announced on a free lock.
        // So we either get the lock here, or Exit() knows to hand it to a waiter.
        while (true)
        {
            int s = _state.Value;

            // free -> held
            if ((s & (_lockBit | _disposeBit)) == 0)
            {
                if (_state.TrySet(s | _lockBit, s))
                {
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.TrySet(s + _waitersValue, s))
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if ((_state.Value & _disposeBit) != 0)
            {
                _state.Add(-_waitersValue); // undo announce
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                _state.Add(-_waitersValue); // undo announce
                return ValueTask.FromCanceled<Releaser>(cancellationToken);
            }
            
            ClaimWaiter(out handle);
        }

        return handle.NewValueTask(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken()
    {
        // See notes in LockSlow()

        while (true)
        {
            int s = _state.Value;

            // free -> held
            if ((s & (_lockBit | _disposeBit)) == 0)
            {
                if (_state.TrySet(s | _lockBit, s))
                {
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            if (_state.TrySet(s + _waitersValue, s))
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if ((_state.Value & _disposeBit) != 0)
            {
                _state.Add(-_waitersValue);
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            ClaimWaiter(out handle);
        }

        return handle.NewValueTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryLock(out Releaser releaser)
    {
        if (_state.TrySet(_lockBit, _availableNoWaiters))
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
        if ((_state.Value & _disposeBit) != 0)
            throw new ObjectDisposedException(nameof(AsyncLock));

        cancellationToken.ThrowIfCancellationRequested();

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(_lockBit, _availableNoWaiters))
            return new Releaser(this);

        return LockSyncSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(CancellationToken cancellationToken)
    {
        // See notes in LockSlow()

        while (true)
        {
            int s = _state.Value;
            
            if ((s & (_lockBit | _disposeBit)) == 0)
            {
                if (_state.TrySet(s | _lockBit, s))
                {
                    return new Releaser(this);
                }

                continue;
            }
            
            if (_state.TrySet(s + _waitersValue, s))
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if ((_state.Value & _disposeBit) != 0)
            {
                _state.Add(-_waitersValue);
                throw new ObjectDisposedException(nameof(AsyncLock));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                _state.Add(-_waitersValue);
                throw new OperationCanceledException(cancellationToken);
            }

            ClaimWaiter(out handle);
        }

        return handle.NewValueTask(cancellationToken).AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        // Assumption: If this lock is implemented correctly, there should never be
        // a race to release the lock, since there should only ever be one owner.

        while (true)
        {
            var s = _state.Value;

            // If there are waiters, take the slow path
            if ((s & ~(_lockBit | _disposeBit)) != 0)
                break;

            // Fast path: held (1) -> free (0), no waiters announced
            if (_state.TrySet(s & ~_lockBit, s))
            {
                // It's possible for the lock to be reacquired very quickly,
                // so make sure to only complete the dispose waiter if the lock is free.
                if ((_state.Value & (_lockBit | _disposeBit)) == _disposeBit)
                {
                    // _disposeWaiter must be checked/completed under _gate,
                    // otherwise DisposeAsync() can create it under the lock and we can miss it.
                    lock (_gate)
                    {
                        _disposeWaiter?.TrySetResult();
                        _disposeWaiter = null;
                    }
                }

                return;
            }
        }

        // Slow path: waiters announced
        while (true)
        {
            WaiterHandle handle;
            bool pop = false;

            lock (_gate)
            {
                if (pop)
                {
                    // The head of the queue was already completed.
                    PopWaiter();
                    pop = false;
                }
                
                if (_waiterQueueHead.Next is null)
                {
                    // A Lock method has announced a waiter, but has not yet claimed the
                    // spare waiter in the queue. In this instance, it is okay to complete the
                    // waiter without pushing another. There will not be another call to Exit()
                    // until the Lock method completes and maintains the queue invariant.
                    _state.Add(-_waitersValue);
                    
                    if ((_state.Value & _disposeBit) != 0)
                    {
                        _disposeWaiter?.TrySetResult();
                        _disposeWaiter = null;
                    }
                    else
                    {
                        _waiterQueueHead.Processed = true;
                        _waiterQueueHead.TryGrant(new Releaser(this));
                    }

                    return;
                }

                handle = NextWaiter();
                _state.Add(-_waitersValue);
            }

            if ((_state.Value & _disposeBit) != 0)
            {
                // No longer transferring ownership of lock.
                // Dispose() took care of the other waiters, so just fault the one we have
                // here and then clean up.
                handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
                _state.Value = _disposeBit;
                
                lock (_gate)
                {
                    _disposeWaiter?.TrySetResult();
                    _disposeWaiter = null;
                }

                return;
            }

            // Try to hand the lock over to the next waiter
            if (handle.TryGrant(new Releaser(this))) return;
            
            // If the waiter is already completed, then it is safe to assume it
            // was already claimed by a call to a Lock() method.
            pop = true;
        }
    }

    public void Dispose()
    {
        WaiterHandle handle;
        
        lock (_gate)
        {
            if ((_state.Or(_disposeBit) & _disposeBit) != 0) return;

            // Clear waiter count
            _state.And(_lockBit | _disposeBit);

            // Take the entire queue, leave behind the spare waiter
            // to maintain the queue invariant.
            handle = _waiterQueueHead;
            _waiterQueueHead = _waiterQueueTail;
        }
        
        // Fault each waiter
        var ode = new ObjectDisposedException(nameof(AsyncLock));
        while (handle.Next is not null)
        {
            handle.TrySetException(ode);
            handle = handle.Next;
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();

        lock (_gate)
        {
            // If held, create the task to wait for the lock to be free, on the next call to Exit()
            if ((_state.Value & _lockBit) != 0)
            {
                _disposeWaiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return new ValueTask(_disposeWaiter.Task);
            }
        }
        
        return ValueTask.CompletedTask;
    }
}
