using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node, backed by ManualResetValueTaskSourceCore to yield a ValueTask.
/// </summary>
internal sealed class Waiter : IValueTaskSource<Releaser>
{
    private static readonly ConcurrentBag<WaiterHandle> _pool = [];

    private static readonly Action<object?> _cancelCallback = CancelCallback;
    private static void CancelCallback(object? handle) => ((WaiterHandle) handle!).Cancel();

    private ManualResetValueTaskSourceCore<Releaser> _core = new() { RunContinuationsAsynchronously = true };
    private bool _completed;
    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    private Waiter()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static WaiterHandle Rent()
    {
        return _pool.TryTake(out var waiter) ? waiter : new WaiterHandle(new Waiter(), 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask() => new(this, _core.Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask(WaiterHandle handle, CancellationToken token)
    {
        RegisterCancellation(handle, token);
        return AsValueTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RegisterCancellation(WaiterHandle handle, CancellationToken token)
    {
        if (!token.CanBeCanceled) return;

        // Cheap fast-check to avoid registration if already canceled.
        if (token.IsCancellationRequested)
        {
            CancelCore(token);
            return;
        }

        // Registration is comparatively expensive: do it last.
        _token = token;
        _ctr = token.UnsafeRegister(_cancelCallback, handle);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CancelCore(CancellationToken token)
    {
        _core.SetException(new OperationCanceledException(token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete(short version)
    {
        return version == _core.Version && !Interlocked.CompareExchange(ref _completed, true, false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Cancel(short version)
    {
        if (!TryComplete(version)) return;
        CancelCore(_token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser, short version)
    {
        if (!TryComplete(version)) return false;
        _core.SetResult(releaser);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception ex, short version)
    {
        if (!TryComplete(version)) return;
        _core.SetException(ex);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CleanupCancellation()
    {
        _ctr.Dispose();
        _ctr = default;
        _token = CancellationToken.None;
    }

    public Releaser GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            // Once the task is consumed, it is safe to return this waiter to the pool.
            CleanupCancellation();
            _core.Reset();
            Volatile.Write(ref _completed, false);
            _pool.Add(new WaiterHandle(this, _core.Version));
        }
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
