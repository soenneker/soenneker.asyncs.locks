using Soenneker.Asyncs.Locks.Abstract;
using Soenneker.Atomics.ValueBools;
using Soenneker.Atomics.ValueInts;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Extensions.Task;

// ReSharper disable InconsistentlySynchronizedField

namespace Soenneker.Asyncs.Locks;

///<inheritdoc cref="IAsyncLock"/>
public sealed class AsyncLock : IAsyncLock
{
    private ValueAtomicInt _state;
    private ValueAtomicBool _disposed;

    // Faster to use than Lock currently in .net 10
    // ReSharper disable once ChangeFieldTypeToSystemThreadingLock
    private readonly object _gate = new();
    private readonly Queue<Waiter> _queue = new();

    // Completed when the lock is free. Reset when the lock is taken.
    private TaskCompletionSource<object?>? _disposeWaiter;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> Lock(CancellationToken cancellationToken)
    {
        if (_disposed.Value)
            return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromException<Releaser>(new OperationCanceledException(cancellationToken));

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

        if (_state.TrySet(1, 0))
            return new ValueTask<Releaser>(new Releaser(this));

        return LockSlowNoToken();
    }

    private ValueTask<Releaser> LockSlow(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromException<Releaser>(new OperationCanceledException(cancellationToken));

        Waiter waiter = Waiter.Rent();

        lock (_gate)
        {
            if (_disposed.Value)
            {
                waiter.Return();
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            // Re-check: free -> held
            if (_state.TrySet(1, 0))
            {
                waiter.Return();
                return new ValueTask<Releaser>(new Releaser(this));
            }

            _queue.Enqueue(waiter);
            _state.Add(2); // increment waiter count
        }

        waiter.RegisterCancellation(cancellationToken);
        return waiter.AsValueTask();
    }

    private ValueTask<Releaser> LockSlowNoToken()
    {
        Waiter waiter = Waiter.Rent();

        lock (_gate)
        {
            if (_disposed.Value)
            {
                waiter.Return();
                return ValueTask.FromException<Releaser>(new ObjectDisposedException(nameof(AsyncLock)));
            }

            if (_state.TrySet(1, 0))
            {
                waiter.Return();
                return new ValueTask<Releaser>(new Releaser(this));
            }

            _queue.Enqueue(waiter);
            _state.Add(2);
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

        if (_state.TrySet(1, 0))
            return new Releaser(this);

        return LockSyncSlow(cancellationToken);
    }

    private Releaser LockSyncSlow(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Waiter waiter = Waiter.RentSync();

        lock (_gate)
        {
            if (_disposed.Value)
            {
                waiter.Return();
                throw new ObjectDisposedException(nameof(AsyncLock));
            }

            if (_state.TrySet(1, 0))
            {
                waiter.Return();
                return new Releaser(this);
            }

            _queue.Enqueue(waiter);
            _state.Add(2);
        }

        waiter.RegisterCancellation(cancellationToken);

        try
        {
            return waiter.WaitSync();
        }
        finally
        {
            waiter.Return();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Exit()
    {
        // FAST PATH:
        // held (1) -> free (0), no waiters
        if (_state.CompareExchange(0, 1) == 1)
        {
            if (_disposed.Value && _disposeWaiter is not null)
            {
                lock (_gate)
                {
                    _disposeWaiter?.TrySetResult(null);
                    _disposeWaiter = null;
                }
            }

            return;
        }

        // SLOW PATH: waiters exist
        while (true)
        {
            Waiter? next;
            bool isDisposed;

            lock (_gate)
            {
                isDisposed = _disposed.Value;

                if (_queue.Count == 0)
                {
                    _state.VolatileWrite(0);

                    if (isDisposed && _disposeWaiter is not null)
                    {
                        _disposeWaiter.TrySetResult(null);
                        _disposeWaiter = null;
                    }

                    return;
                }

                next = _queue.Dequeue();
                _state.Add(-2); // remove one waiter
            }

            if (isDisposed)
            {
                next.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
                next.Return();
                return;
            }

            if (next.TryGrant(new Releaser(this)))
                return;

            next.Return();
        }
    }

    public void Dispose()
    {
        if (!_disposed.TrySetTrue())
            return;

        lock (_gate)
        {
            if ((_state.Read() & 1) == 0 && _disposeWaiter is not null)
            {
                _disposeWaiter.TrySetResult(null);
                _disposeWaiter = null;
            }
        }

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

            w.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
            w.Return();
        }
    }

    public async ValueTask DisposeAsync()
    {
        Dispose();

        Task? waitTask = null;

        lock (_gate)
        {
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