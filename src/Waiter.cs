using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Soenneker.Atomics.ValueInts;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node, backed by ManualResetValueTaskSourceCore to yield a ValueTask.
/// </summary>
internal sealed class Waiter : IValueTaskSource<Releaser>
{
    private const int _completedBit = 1 << 16;
    
    private static readonly ConcurrentBag<WaiterHandle> _pool = [];

    private static readonly Action<object?> _cancelCallback = CancelCallback;
    private static void CancelCallback(object? handle) => ((WaiterHandle) handle!).Cancel();

    /// <summary>
    /// State encoding:
    /// - bit0..15: ValueTask Core version
    /// - bit16: Completed flag (0 = not completed, 1 = completed)
    /// </summary>
    private ValueAtomicInt _state;

    private ManualResetValueTaskSourceCore<Releaser> _core = new() { RunContinuationsAsynchronously = true };
    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    private Waiter()
    { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static WaiterHandle Rent()
    {
        return _pool.TryTake(out var waiter) ? waiter : new Waiter().GetHandle();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private WaiterHandle GetHandle() => new(this, _core.Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask() => new(this, _core.Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask(WaiterHandle handle, CancellationToken cancellationToken)
    {
        RegisterCancellation(handle, cancellationToken);
        return AsValueTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RegisterCancellation(WaiterHandle handle, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
            return;

        // Cheap fast-check to avoid registration if already canceled.
        if (cancellationToken.IsCancellationRequested)
        {
            if (TryComplete(_core.Version))
            {
                CancelCore(cancellationToken);
            }

            return;
        }

        // Registration is comparatively expensive: do it last.
        _token = cancellationToken;
        _ctr = cancellationToken.UnsafeRegister(_cancelCallback, handle);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CancelCore(CancellationToken cancellationToken)
    {
        _core.SetException(new OperationCanceledException(cancellationToken));
    }

    // Ensures that the waiter can only be completed once per version,
    // and that handles to previous versions become stale.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete(short version)
    {
        return _state.TrySet((ushort) version | _completedBit, version);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Cancel(short version)
    {
        if (!TryComplete(version))
            return;

        CancelCore(_token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser, WaiterHandle handle)
    {
        if (!TryComplete(handle.Version))
            return false;

        handle.MarkGranted();
        _core.SetResult(releaser);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception ex, short version)
    {
        if (!TryComplete(version))
            return;

        _core.SetException(ex);
    }

    public Releaser GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            // Once the task is consumed, it is safe to reset and return this waiter to the pool.
            _ctr.Dispose();
            _ctr = default;
            _token = CancellationToken.None;
            
            _core.Reset();
            _state.Value = _core.Version;
            _pool.Add(GetHandle());
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
