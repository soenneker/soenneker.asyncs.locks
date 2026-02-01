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
    /// - bits1..: acquire count * 2 (including current holder, so we can add/subtract 2 per waiter while preserving bit0)
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

    // Initialize queue with the spare waiter
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
        if (cancellationToken.IsCancellationRequested)
        {
            if ((_state.Value & _disposeBit) != 0)
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            
            return ValueTask.FromCanceled<Releaser>(cancellationToken);
        }

        // Fast path: free -> held
        var state = _state.Add(_lockValue);
        
        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));

        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return LockSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock()
    {
        // Fast path: free -> held
        var state = _state.Add(_lockValue);
        
        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));
        
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return LockSlowNoToken();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(CancellationToken cancellationToken)
    {
        WaiterHandle handle;

        lock (_gate)
        {
            ClaimWaiter(out handle);
        }
        
        if ((_state.Value & _disposeBit) != 0)
        {
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return handle.NewValueTask(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken()
    {
        WaiterHandle handle;

        lock (_gate)
        {
            ClaimWaiter(out handle);
        }
        
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
        if (cancellationToken.IsCancellationRequested)
        {
            if ((_state.Value & _disposeBit) != 0)
                throw new ObjectDisposedException(nameof(AsyncLock));
            
            throw new OperationCanceledException(cancellationToken);
        }

        // Fast path: free -> held
        var state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new Releaser(this);
        
        if ((state & _disposeBit) != 0)
            throw new ObjectDisposedException(nameof(AsyncLock));

        return LockSyncSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(CancellationToken cancellationToken)
    {
        WaiterHandle handle;

        lock (_gate)
        {
            ClaimWaiter(out handle);
        }
        
        if ((_state.Value & _disposeBit) != 0)
        {
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }

        return handle.NewValueTask(cancellationToken).AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        // Assumption: If this lock is implemented correctly, there should never be
        // a race to release the lock, since there should only ever be one owner.

        if (_state.Add(-_lockValue) is var state and < _lockValue)
        {
            if ((state & _disposeBit) != 0)
            {
                lock (_gate)
                {
                    _disposeWaiter?.TrySetResult();
                    _disposeWaiter = null;
                }
            }

            return;
        }

        // Slow path: waiters announced
        while (true)
        {
            WaiterHandle handle;
            var pop = false;

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
            }

            if ((_state.Value & _disposeBit) != 0)
            {
                // No longer transferring ownership of lock.
                // Dispose() took care of the other waiters, so just fault the one we have
                // here and then clean up.
                handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
                
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
            var state = _state.Exchange(_disposeBit + _lockValue);

            if ((state & _disposeBit) != 0)
                return;

            if (state != _availableNoWaiters)
            {
                _disposeWaiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

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
            return _disposeWaiter is null ? ValueTask.CompletedTask : new ValueTask(_disposeWaiter.Task);
        }
    }
}
