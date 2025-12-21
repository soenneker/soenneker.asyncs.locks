using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Soenneker.Atomics.ValueBools;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node:
/// - async mode: ManualResetValueTaskSourceCore (pooled)
/// - sync mode: TaskCompletionSource (allocated only when contended sync)
/// </summary>
internal sealed class Waiter : IValueTaskSource<Releaser>
{
    private static readonly ConcurrentBag<Waiter> _pool = new();

    private ManualResetValueTaskSourceCore<Releaser> _core = new() { RunContinuationsAsynchronously = true };

    private TaskCompletionSource<Releaser>? _tcs; // sync-only

    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    // 0 = active, 1 = canceled
    private ValueAtomicBool _canceled;

    // 0 = async, 1 = sync
    private int _kind;

    public bool IsCanceled => _canceled.Value;

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

        w._canceled.Value = false;
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
        w._canceled.Value = false;
        w._core.Reset();
        return w;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask() => new(this, _core.Version);

    public void RegisterCancellation(CancellationToken token)
    {
        if (!token.CanBeCanceled)
            return;

        _token = token;

        // This check is cheap and avoids registration in the already-canceled case
        if (token.IsCancellationRequested)
        {
            Cancel();
            return;
        }

        // Registration is expensive: do it last
        _ctr = token.Register(static s => ((Waiter)s!).Cancel(), this);
    }

    private void Cancel()
    {
        if (!_canceled.TrySetTrue())
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
        if (_canceled.Value)
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
        return _tcs!.Task.GetAwaiter()
                    .GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Return()
    {
        CleanupCancellation();

        _tcs = null;
        _kind = 0;

        _canceled.Value = false;
        _pool.Add(this);
    }

    // IValueTaskSource<Releaser>
    Releaser IValueTaskSource<Releaser>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<Releaser>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<Releaser>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}