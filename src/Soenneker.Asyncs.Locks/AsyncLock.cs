using Soenneker.Asyncs.Locks.Abstract;
using Soenneker.Queues.Intrusive.ValueMpsc;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks;

/// <inheritdoc cref="IAsyncLock" />
public sealed class AsyncLock : IAsyncLock
{
    private const long _countMask = uint.MaxValue;
    private const long _disposedBit = 1L << 32;
    private const long _consumerBit = 1L << 33;
    private const long _overflowBit = 1L << 34;

    // Low 32 bits: holder plus announced waiters. High bits: disposal, queue-consumer ownership, and overflow mode.
    private long _state;
    private Waiter? _frontWaiter;
    private ValueIntrusiveMpscReclaimingQueue<Waiter> _waiterQueue;
    private TaskCompletionSource? _disposeWaiter;

    public AsyncLock()
    {
        Waiter stub = Waiter.Rent();
        stub.Next = null;
        _waiterQueue = new ValueIntrusiveMpscReclaimingQueue<Waiter>(stub);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock()
    {
        int admission = Announce();

        if (admission < 0)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        if (admission == 0)
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlow(admission == 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock(CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
            return Lock();

        if (cancellationToken.IsCancellationRequested)
        {
            if (IsDisposed(Volatile.Read(ref _state)))
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

            return ValueTask.FromCanceled<Releaser>(cancellationToken);
        }

        int admission = Announce();

        if (admission < 0)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        if (admission == 0)
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlow(cancellationToken, admission == 1);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(bool useFrontSlot)
    {
        Waiter waiter = Waiter.Rent();
        ValueTask<Releaser> result = waiter.NewValueTask();
        Publish(waiter, useFrontSlot);
        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlow(CancellationToken cancellationToken, bool useFrontSlot)
    {
        Waiter waiter = Waiter.Rent();
        ValueTask<Releaser> result = waiter.NewValueTask(cancellationToken);
        Publish(waiter, useFrontSlot);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Announce()
    {
        long state = Volatile.Read(ref _state);

        while (true)
        {
            if (IsDisposed(state))
                return -1;

            int count = GetCount(state);
            long observed = Interlocked.CompareExchange(ref _state, WithCount(state, count + 1), state);

            if (observed == state)
            {
                if (count == 0)
                    return 0;

                // A count of one without an active consumer proves that only the current
                // holder precedes us, so this caller uniquely owns the direct slot.
                return count == 1 && !HasConsumer(state) && (state & _overflowBit) == 0 ? 1 : 2;
            }

            state = observed;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Publish(Waiter waiter, bool useFrontSlot)
    {
        if (useFrontSlot)
        {
            Volatile.Write(ref _frontWaiter, waiter);
            return;
        }

        if ((Volatile.Read(ref _state) & _overflowBit) == 0)
            Interlocked.Or(ref _state, _overflowBit);

        _waiterQueue.Enqueue(waiter);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryLock(out Releaser releaser)
    {
        long observed = Interlocked.CompareExchange(ref _state, 1, 0);

        if (observed == 0)
        {
            releaser = new Releaser(this);
            return true;
        }

        if (observed == _overflowBit && Interlocked.CompareExchange(ref _state, _overflowBit | 1, _overflowBit) == _overflowBit)
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
        if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested)
        {
            if (IsDisposed(Volatile.Read(ref _state)))
                throw new ObjectDisposedException(nameof(AsyncLock));

            throw new OperationCanceledException(cancellationToken);
        }

        int admission = Announce();

        if (admission < 0)
            throw new ObjectDisposedException(nameof(AsyncLock));

        return admission == 0 ? new Releaser(this) : LockSyncSlow(cancellationToken, admission == 1);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Releaser LockSyncSlow(CancellationToken cancellationToken, bool useFrontSlot)
    {
        Waiter waiter = Waiter.Rent();
        ValueTask<Releaser> result = waiter.NewValueTask(cancellationToken);
        Publish(waiter, useFrontSlot);
        return result.AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        long state = Volatile.Read(ref _state);

        // With exactly one non-cancellable waiter, the release CAS itself can
        // linearize ownership transfer. The published front pointer prevents a
        // following waiter from reusing the slot until we detach this one.
        if (state == 2)
        {
            Waiter? waiter = Volatile.Read(ref _frontWaiter);

            if (waiter is not null && waiter.CanGrantDirectly && Interlocked.CompareExchange(ref _state, 1, 2) == 2)
            {
                Volatile.Write(ref _frontWaiter, null);
                waiter.CompleteGrant(new Releaser(this));
                return;
            }
        }

        var spinner = new SpinWait();

        while (true)
        {
            if (HasConsumer(state))
            {
                spinner.SpinOnce();
                state = Volatile.Read(ref _state);
                continue;
            }

            int count = GetCount(state);

            if (count == 0)
                return;

            int remaining = count - 1;
            bool consume = remaining != 0 && !IsDisposed(state);
            long updated = WithCount(state, remaining);

            if (consume)
                updated |= _consumerBit;

            long observed = Interlocked.CompareExchange(ref _state, updated, state);

            if (observed != state)
            {
                state = observed;
                continue;
            }

            if (consume)
                GrantNext();
            else if (remaining == 0 && IsDisposed(state))
                Volatile.Read(ref _disposeWaiter)?.TrySetResult();

            return;
        }
    }

    private void GrantNext()
    {
        while (true)
        {
            Waiter waiter = DequeueWaiter();

            if (waiter.TryReserveGrant())
            {
                Interlocked.And(ref _state, ~_consumerBit);
                waiter.CompleteGrant(new Releaser(this));
                return;
            }

            waiter.MarkDequeued();
            long state = Volatile.Read(ref _state);

            while (true)
            {
                int remaining = GetCount(state) - 1;
                long updated = WithCount(state, remaining);

                if (remaining == 0)
                    updated &= ~_consumerBit;

                long observed = Interlocked.CompareExchange(ref _state, updated, state);

                if (observed == state)
                {
                    if (remaining == 0)
                        return;

                    break;
                }

                state = observed;
            }
        }
    }

    private Waiter DequeueWaiter()
    {
        var spinner = new SpinWait();

        while (true)
        {
            Waiter? waiter = TakeFrontWaiter();

            if (waiter is not null)
                return waiter;

            if (_waiterQueue.TryDequeueSpinUntilLinked(out waiter))
                return waiter;

            spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Waiter? TakeFrontWaiter()
    {
        Waiter? waiter = Volatile.Read(ref _frontWaiter);

        if (waiter is not null)
            Volatile.Write(ref _frontWaiter, null);

        return waiter;
    }

    public void Dispose()
    {
        long state = Volatile.Read(ref _state);
        var spinner = new SpinWait();

        while (true)
        {
            if (IsDisposed(state))
                return;

            if (HasConsumer(state))
            {
                spinner.SpinOnce();
                state = Volatile.Read(ref _state);
                continue;
            }

            long updated = state | _disposedBit | _consumerBit;
            long observed = Interlocked.CompareExchange(ref _state, updated, state);

            if (observed == state)
                break;

            state = observed;
        }

        int count = GetCount(state);
        int queued = count > 0 ? count - 1 : 0;
        var exception = new ObjectDisposedException(nameof(AsyncLock));

        for (var i = 0; i < queued; i++)
        {
            Waiter waiter = DequeueWaiter();
            waiter.TrySetException(exception);
            waiter.MarkDequeued();
        }

        int holderCount = count > 0 ? 1 : 0;
        Volatile.Write(ref _state, _disposedBit | (uint)holderCount);

        if (holderCount == 0)
            Volatile.Read(ref _disposeWaiter)?.TrySetResult();
    }

    public ValueTask DisposeAsync()
    {
        long state = Volatile.Read(ref _state);

        if (IsDisposed(state) && GetCount(state) == 0)
            return ValueTask.CompletedTask;

        TaskCompletionSource waiter = GetDisposeWaiter();
        Dispose();

        state = Volatile.Read(ref _state);
        if (IsDisposed(state) && GetCount(state) == 0)
            waiter.TrySetResult();

        return new ValueTask(waiter.Task);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private TaskCompletionSource GetDisposeWaiter()
    {
        TaskCompletionSource? waiter = Volatile.Read(ref _disposeWaiter);
        if (waiter is not null)
            return waiter;

        var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return Interlocked.CompareExchange(ref _disposeWaiter, created, null) ?? created;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCount(long state) => unchecked((int)state);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDisposed(long state) => (state & _disposedBit) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasConsumer(long state) => (state & _consumerBit) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long WithCount(long state, int count) => (state & ~_countMask) | (uint)count;
}
