using Soenneker.Atomics.ValueInts;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node:
/// - async mode: ManualResetValueTaskSourceCore (pooled)
/// - sync mode: TaskCompletionSource (allocated only when contended sync)
/// </summary>
internal sealed class Waiter : IValueTaskSource<Releaser>
{
    private static readonly ConcurrentBag<Waiter> _pool = [];

    private ManualResetValueTaskSourceCore<Releaser> _core = new() { RunContinuationsAsynchronously = true };

    private TaskCompletionSource<Releaser>? _tcs; // sync-only

    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    // completion state:
    // 0 = pending, 1 = canceled, 2 = signaled (result/exception)
    private ValueAtomicInt _completion;

    // lifecycle bit flags:
    // 1 = dequeued from AsyncLock queue
    // 2 = consumed by waiter (awaited or sync waited)
    private int _lifecycle;

    // 0 = async, 1 = sync
    private int _kind;

    private Waiter()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Waiter Rent()
    {
        if (!_pool.TryTake(out Waiter? w))
            w = new Waiter();

        w._kind = 0;
        w._tcs = null;
        w._token = CancellationToken.None;
        w._ctr = default;

        w._lifecycle = 0;
        w._completion.Value = 0;
        w._core.Reset();

        return w;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Waiter RentSync()
    {
        if (!_pool.TryTake(out Waiter? w))
            w = new Waiter();

        w._kind = 1;
        w._tcs = new TaskCompletionSource<Releaser>(TaskCreationOptions.RunContinuationsAsynchronously);
        w._token = CancellationToken.None;
        w._ctr = default;

        w._lifecycle = 0;
        w._completion.Value = 0;
        w._core.Reset();

        return w;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask() => new(this, _core.Version);

    /// <summary>
    /// Called by <see cref="AsyncLock"/> after this waiter is removed from its internal queue.
    /// Returns false if the waiter was already consumed (e.g., cancellation observed) and must not be touched further.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MarkDequeued()
    {
        int prev = Interlocked.Or(ref _lifecycle, 1);

        // If already consumed, it's already been observed by the waiter.
        // Return it to the pool now and tell the caller to skip any further interaction.
        if ((prev & 2) != 0)
        {
            Return();
            return false;
        }

        return true;
    }

    /// <summary>
    /// Called by the waiter (awaiter or sync waiter) once completion is observed.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkConsumed()
    {
        int prev = Interlocked.Or(ref _lifecycle, 2);

        // If already dequeued, safe to return to pool now.
        if ((prev & 1) != 0)
            Return();
    }

    public void RegisterCancellation(CancellationToken token)
    {
        if (!token.CanBeCanceled)
            return;

        _token = token;

        // Cheap fast-check to avoid registration if already canceled.
        if (token.IsCancellationRequested)
        {
            Cancel();
            return;
        }

        // Registration is comparatively expensive: do it last.
        _ctr = token.UnsafeRegister(static s => ((Waiter)s!).Cancel(), this);
    }

    private void Cancel()
    {
        if (_completion.CompareExchange(1, 0) != 0)
            return;

        var oce = new OperationCanceledException(_token);

        if (_kind == 1)
        {
            _tcs!.TrySetException(oce);
            CleanupCancellation();
            return;
        }

        short v = _core.Version;
        if (_core.GetStatus(v) == ValueTaskSourceStatus.Pending)
            _core.SetException(oce);

        CleanupCancellation();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser)
    {
        if (_completion.CompareExchange(2, 0) != 0)
            return false;

        try
        {
            if (_kind == 1)
                return _tcs!.TrySetResult(releaser);

            short v = _core.Version;
            if (_core.GetStatus(v) != ValueTaskSourceStatus.Pending)
                return false;

            _core.SetResult(releaser);
            return true;
        }
        finally
        {
            CleanupCancellation();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception ex)
    {
        if (_completion.CompareExchange(2, 0) != 0)
            return;

        try
        {
            if (_kind == 1)
            {
                _tcs?.TrySetException(ex);
                return;
            }

            short v = _core.Version;
            if (_core.GetStatus(v) == ValueTaskSourceStatus.Pending)
                _core.SetException(ex);
        }
        finally
        {
            CleanupCancellation();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CleanupCancellation()
    {
        _ctr.Dispose();
        _ctr = default;
        _token = CancellationToken.None;
    }

    public Releaser WaitSync()
    {
        // Blocks the calling thread. Continuations are async (RunContinuationsAsynchronously).
        return _tcs!.Task.GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Return()
    {
        CleanupCancellation();

        _tcs = null;
        _kind = 0;
        _lifecycle = 0;
        _completion.Value = 0;
        _pool.Add(this);
    }

    // IValueTaskSource<Releaser>
    Releaser IValueTaskSource<Releaser>.GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            MarkConsumed();
        }
    }

    ValueTaskSourceStatus IValueTaskSource<Releaser>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<Releaser>.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}
