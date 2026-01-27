using Soenneker.Asyncs.Locks.Abstract;
using Soenneker.Atomics.ValueBools;
using Soenneker.Atomics.ValueInts;
using Soenneker.Extensions.Task;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable InconsistentlySynchronizedField

namespace Soenneker.Asyncs.Locks;

/// <inheritdoc cref="IAsyncLock"/>
public sealed class AsyncLock : IAsyncLock, IDisposable, IAsyncDisposable
{
    /// <summary>
    /// State encoding:
    /// - bit0: held flag (0 = free, 1 = held)
    /// - bits1..: announced waiter count * 2 (so we can add/subtract 2 per waiter while preserving bit0)
    /// </summary>
    private ValueAtomicInt _state;

    private ValueAtomicBool _disposed;

    // Faster to use than Lock currently in .NET 10
    // ReSharper disable once ChangeFieldTypeToSystemThreadingLock
    private readonly object _gate = new();

    // Invariant: Outside of gate, queue always contains one extra waiter for the next async lock access.
    private WaiterHandle _waiterQueueHead;
    private WaiterHandle _waiterQueueTail;

    // Used by DisposeAsync() to wait until the lock becomes free after Dispose() has been called.
    // Must only be accessed under _gate to avoid races.
    private TaskCompletionSource? _disposeWaiter;

    public AsyncLock() => _waiterQueueHead = _waiterQueueTail = Waiter.Rent();
    
    // Add a waiter to the queue.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PushWaiter() => _waiterQueueTail = _waiterQueueTail.Next = Waiter.Rent();

    // Remove the head of the queue.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PopWaiter()
    {
        var handle = _waiterQueueHead;
        _waiterQueueHead = handle.Next!;
        handle.Next = null;
    }

    // Used in Lock methods when handing out a waiter to a consumer.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PopIfProcessed(WaiterHandle handle)
    {
        if (handle.Processed)
        {
            // If the waiter has already been completed (in Exit()), then it is safe
            // to assume the handle is at the head of the queue.
            PopWaiter();
        }
        else
        {
            handle.Processed = true;
        }
    }

    // Same as PopIfProcessed(), but returns the head of the queue.
    // Used in Exit() when handing over control of the lock.
    // Assumes gate lock is held.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WaiterHandle NextWaiter()
    {
        var handle = _waiterQueueHead;

        // If the waiter has already been handed out, pop the queue.
        // Otherwise, leave the waiter in the queue. The active Lock method will hand
        // it to the consumer and take responsibility for popping the queue.
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
        if (_disposed.Value)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<Releaser>(cancellationToken);

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(1, 0))
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock()
    {
        if (_disposed.Value)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(1, 0))
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlowNoToken();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<Releaser>(cancellationToken);

        // Safe "announce" protocol:
        // - If the lock becomes free, acquire it here without queueing.
        // - Otherwise, increment waiter count *before* queueing so Exit()'s "queue empty but waiters announced"
        //   path can never miss us.
        while (true)
        {
            int s = _state.Read();

            // free -> held
            if ((s & 1) == 0)
            {
                if (_state.CompareExchange(s | 1, s) == s)
                {
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            handle = _waiterQueueTail;
            PushWaiter();
            PopIfProcessed(handle);
        }

        return handle.NewValueTask(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken()
    {
        while (true)
        {
            int s = _state.Read();

            // free -> held
            if ((s & 1) == 0)
            {
                if (_state.CompareExchange(s | 1, s) == s)
                {
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            handle = _waiterQueueTail;
            PushWaiter();
            PopIfProcessed(handle);
        }

        return handle.NewValueTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryLock(out Releaser releaser)
    {
        if (_disposed.Value)
        {
            releaser = default;
            return false;
        }

        if (_state.TrySet(1, 0))
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
        if (_disposed.Value)
            throw new ObjectDisposedException(nameof(AsyncLock));

        cancellationToken.ThrowIfCancellationRequested();

        // Fast path: free (0) -> held (1)
        if (_state.TrySet(1, 0))
            return new Releaser(this);

        return LockSyncSlow(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (true)
        {
            int s = _state.Read();

            // free -> held
            if ((s & 1) == 0)
            {
                if (_state.CompareExchange(s | 1, s) == s)
                {
                    return new Releaser(this);
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        WaiterHandle handle;

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                throw new ObjectDisposedException(nameof(AsyncLock));
            }

            handle = _waiterQueueTail;
            PushWaiter();
            PopIfProcessed(handle);
        }

        return handle.NewValueTask(cancellationToken).AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        // Fast path: held (1) -> free (0), no waiters announced.
        if (_state.CompareExchange(0, 1) == 1)
        {
            // Critical fix: _disposeWaiter must be checked/completed under _gate,
            // otherwise DisposeAsync() can create it under the lock and we can miss it.
            if (_disposed.Value)
            {
                lock (_gate)
                {
                    _disposeWaiter?.TrySetResult();
                    _disposeWaiter = null;
                }
            }

            return;
        }

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
                
                // The queue is empty. Do not complete the extra buffer element
                // to maintain the queue invariant.
                if (_waiterQueueHead.Next is null)
                {
                    _state.Value = 0;
                    
                    if (_disposed.Value)
                    {
                        _disposeWaiter?.TrySetResult();
                        _disposeWaiter = null;
                    }

                    return;
                }

                handle = NextWaiter();
                _state.Add(-2);
            }

            if (_disposed.Value)
            {
                handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
            }

            // Try to hand the lock over to the next waiter
            if (handle.TryGrant(new Releaser(this))) return;
            
            // If the waiter is already completed, then it is safe to assume any
            // Lock method waiting to pull it off the queue had already completed.
            pop = true;
        }
    }

    public void Dispose()
    {
        WaiterHandle handle;
        
        lock (_gate)
        {
            if (!_disposed.TrySetTrue())
                return;

            // Take the entire queue, leave behind the extra element
            // to maintain the queue invariant.
            handle = _waiterQueueHead;
            _waiterQueueHead = _waiterQueueTail;

            // Clear the waiter count
            while (true)
            {
                var s = _state.Read();
                if (_state.TrySet(s & 1, s)) break;
            }
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
            // If held, we need to wait until Exit() transitions it to free and signals.
            if ((_state.Read() & 1) != 0)
            {
                _disposeWaiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return new ValueTask(_disposeWaiter.Task);
            }
        }
        
        return ValueTask.CompletedTask;
    }
}
