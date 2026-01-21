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
    private readonly Queue<Waiter> _queue = new();

    // Used by DisposeAsync() to wait until the lock becomes free after Dispose() has been called.
    // Must only be accessed under _gate to avoid races.
    private TaskCompletionSource<object?>? _disposeWaiter;

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

        Waiter waiter = Waiter.Rent();

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
                    waiter.Return();
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                waiter.Return();
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            _queue.Enqueue(waiter);
        }

        waiter.RegisterCancellation(cancellationToken);
        return waiter.AsValueTask();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<Releaser> LockSlowNoToken()
    {
        Waiter waiter = Waiter.Rent();

        while (true)
        {
            int s = _state.Read();

            // free -> held
            if ((s & 1) == 0)
            {
                if (_state.CompareExchange(s | 1, s) == s)
                {
                    waiter.Return();
                    return new ValueTask<Releaser>(new Releaser(this));
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                waiter.Return();
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            _queue.Enqueue(waiter);
        }

        return waiter.AsValueTask();
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

        Waiter waiter = Waiter.RentSync();

        while (true)
        {
            int s = _state.Read();

            // free -> held
            if ((s & 1) == 0)
            {
                if (_state.CompareExchange(s | 1, s) == s)
                {
                    waiter.Return();
                    return new Releaser(this);
                }

                continue;
            }

            // held -> held with +1 waiter (announced)
            if (_state.CompareExchange(s + 2, s) == s)
                break;
        }

        lock (_gate)
        {
            if (_disposed.Value)
            {
                _state.Add(-2); // undo announce
                waiter.Return();
                throw new ObjectDisposedException(nameof(AsyncLock));
            }

            _queue.Enqueue(waiter);
        }

        waiter.RegisterCancellation(cancellationToken);

        try
        {
            return waiter.WaitSync();
        }
        finally
        {
            waiter.MarkConsumed();
        }
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
                    _disposeWaiter?.TrySetResult(null);
                    _disposeWaiter = null;
                }
            }

            return;
        }

        var spin = new SpinWait();
        int retryCount = 0;

        while (true)
        {
            bool retry;

            lock (_gate)
            {
                bool isDisposed = _disposed.Value;
                retry = false;

                if (_queue.Count == 0)
                {
                    // If waiters have been announced but not yet enqueued, avoid losing the wakeup.
                    // Let the enqueuer take the gate and then we'll dequeue/grant on the next iteration.
                    if ((_state.Read() & ~1) != 0)
                    {
                        retry = true;
                    }
                    else
                    {
                        _state.VolatileWrite(0);

                        if (isDisposed)
                        {
                            _disposeWaiter?.TrySetResult(null);
                            _disposeWaiter = null;
                        }

                        return;
                    }
                }
                else
                {
                    Waiter next = _queue.Dequeue();
                    _state.Add(-2); // remove one announced waiter

                    // Removed from queue; if already consumed (e.g. cancellation observed), don't touch it further.
                    if (!next.MarkDequeued())
                    {
                        retryCount = 0;
                        continue;
                    }

                    if (isDisposed)
                    {
                        next.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
                        retryCount = 0;
                        continue;
                    }

                    if (next.TryGrant(new Releaser(this)))
                        return;

                    retryCount = 0;
                    continue;
                }
            }

            if (retry)
            {
                spin.SpinOnce();
                if (++retryCount >= 10)
                {
                    Thread.Yield();
                    retryCount = 0;
                }
                continue;
            }
        }
    }

    public void Dispose()
    {
        // If lock is currently free, complete any existing dispose waiter now.
        lock (_gate)
        {
            if (!_disposed.TrySetTrue())
                return;

            if ((_state.Read() & 1) == 0)
            {
                _disposeWaiter?.TrySetResult(null);
                _disposeWaiter = null;
            }
        }

        // Drain queued waiters and fail them with ODE.
        while (true)
        {
            Waiter? w;

            lock (_gate)
            {
                if (_queue.Count == 0)
                    break;

                w = _queue.Dequeue();
                _state.Add(-2);
            }

            // Removed from queue; if already consumed, don't touch it further.
            if (!w.MarkDequeued())
                continue;

            w.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
        }
    }

    public async ValueTask DisposeAsync()
    {
        Dispose();

        Task? waitTask = null;

        lock (_gate)
        {
            // If held, we need to wait until Exit() transitions it to free and signals.
            if ((_state.Read() & 1) != 0)
            {
                _disposeWaiter ??= new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
                waitTask = _disposeWaiter.Task;
            }
        }

        if (waitTask is not null)
            await waitTask.NoSync();
    }
}
