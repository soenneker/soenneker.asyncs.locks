using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Asyncs.Locks.Abstract;
using Soenneker.Atomics.ValueInts;
using Soenneker.Queues.Intrusive.ValueMpsc;

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
    /// </summary>
    private ValueAtomicInt _state;

    // Waiter queue
    // - many producers (LockSlow*) enqueue waiter nodes
    // - single consumer (Exit) dequeues waiter nodes
    private ValueIntrusiveMpscQueue<WaiterHandle> _waiterQueue;

    private readonly TaskCompletionSource _disposeWaiter = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public AsyncLock()
    {
        // Permanent stub sentinel node for the lifetime of this AsyncLock.
        // Mark as consumed so it will never be awaited and can be recycled when dequeued.
        WaiterHandle stub = Waiter.Rent();
        stub.MarkConsumed();
        stub.Next = null;

        _waiterQueue = new ValueIntrusiveMpscQueue<WaiterHandle>(stub);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnqueueWaiter(out WaiterHandle handle)
    {
        handle = Waiter.Rent();
        handle.Next = null;
        _waiterQueue.Enqueue(handle);
    }

    // Consumer-only dequeue wrapper that also marks the old head dequeued for recycling.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryDequeueWaiter(out WaiterHandle node)
    {
        WaiterHandle oldHead = _waiterQueue.Head;

        // Bounded spin to reduce the tiny "tail swapped but link not yet published" window.
        if (!_waiterQueue.TryDequeueSpin(out node, maxSpins: 16))
            return false;

        // old head is no longer reachable from the queue and can be recycled.
        oldHead.MarkDequeued();
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public ValueTask<Releaser> Lock(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            if ((_state.Value & _disposeBit) != 0)
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

            return ValueTask.FromCanceled<Releaser>(cancellationToken);
        }

        if (_state.Value == _availableNoWaiters && _state.TrySet(_lockValue, _availableNoWaiters))
            return new ValueTask<Releaser>(new Releaser(this));

        int state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlow(state, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public ValueTask<Releaser> Lock()
    {
        if (_state.Value == _availableNoWaiters && _state.TrySet(_lockValue, _availableNoWaiters))
            return new ValueTask<Releaser>(new Releaser(this));

        int state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlowNoToken(state);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(int state, CancellationToken cancellationToken)
    {
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }

        EnqueueWaiter(out WaiterHandle handle);

        // Catch disposal that occurred after we announced but before/after enqueue.
        if ((_state.Value & _disposeBit) != 0)
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));

        return handle.NewValueTask(cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken(int state)
    {
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
        }

        EnqueueWaiter(out WaiterHandle handle);

        if ((_state.Value & _disposeBit) != 0)
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));

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

        int state = _state.Add(_lockValue);

        if (state == _lockValue)
            return new Releaser(this);

        return LockSyncSlow(state, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(int state, CancellationToken cancellationToken)
    {
        if ((state & _disposeBit) != 0)
        {
            _state.Add(-_lockValue);
            throw new ObjectDisposedException(nameof(AsyncLock));
        }

        EnqueueWaiter(out WaiterHandle handle);

        if ((_state.Value & _disposeBit) != 0)
            handle.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));

        return handle.NewValueTask(cancellationToken).AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal void Exit()
    {
        while (true)
        {
            int state = _state.Add(-_lockValue);

            // No announced waiters (0 or disposed-bit only).
            if (state < _lockValue)
            {
                if ((state & _disposeBit) != 0)
                    _disposeWaiter.TrySetResult();

                return;
            }

            if ((_state.Value & _disposeBit) != 0)
            {
                _disposeWaiter.TrySetResult();
                return;
            }

            // We have at least one announced waiter. Dequeue and try to grant.
            // IMPORTANT: If dequeue fails due to the tiny "link window", restore the state we just burned.
            if (!TryDequeueWaiter(out WaiterHandle node))
            {
                _state.Add(_lockValue);
                continue;
            }

            // Try to grant. If canceled/disposed already, TryGrant returns false; loop to attempt next.
            if (node.TryGrant(new Releaser(this)))
                return;
        }
    }

    public void Dispose()
    {
        int state = _state.Exchange(_disposeBit + _lockValue);

        if ((state & _disposeBit) != 0)
            return;

        if (state == _availableNoWaiters)
        {
            _disposeWaiter.TrySetResult();
            return;
        }

        var ode = new ObjectDisposedException(nameof(AsyncLock));

        // Fault all currently linked waiters starting from head.Next.
        // If a producer has swapped tail but not linked prev.Next yet, it will still observe disposed
        // and fault itself via the disposed-bit checks in LockSlow*.
        WaiterHandle head = _waiterQueue.Head;
        WaiterHandle? node = Volatile.Read(ref head.Next);

        while (node is not null)
        {
            node.TrySetException(ode);
            node = Volatile.Read(ref node.Next);
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return new ValueTask(_disposeWaiter.Task);
    }
}
