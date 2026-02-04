using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Soenneker.Atomics.ValueInts;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node backed by ManualResetValueTaskSourceCore to yield a ValueTask.
/// Pooled with a ThreadStatic fast-path and a global lock-free stack fallback.
/// </summary>
internal sealed class Waiter : IValueTaskSource<Releaser>
{
    private const int _completedBit = 1 << 16;

    // ---- Pool ----
    // Global lock-free stack (uses WaiterHandle.Next as the link)
    private static WaiterHandle? _sGlobalPoolHead;

    // Thread-local pool
    [ThreadStatic] private static WaiterHandle? _tLocalPoolHead;
    [ThreadStatic] private static int _tLocalPoolCount;

    // Keep TLS bounded so we don't hoard across threads
    private const int _maxLocal = 32;

    private static readonly Action<object?> _cancelCallback = static s => ((WaiterHandle)s!).Cancel();

    /// <summary>
    /// State encoding:
    /// - bit0..15: ValueTask Core version (ushort)
    /// - bit16: Completed flag (0 = not completed, 1 = completed)
    /// </summary>
    private ValueAtomicInt _state;

    private ManualResetValueTaskSourceCore<Releaser> _core = new()
    {
        RunContinuationsAsynchronously = true
    };

    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    // ✅ One handle per waiter, reused forever (no allocations per completion)
    private readonly WaiterHandle _handle;

    private Waiter()
    {
        _state.Value = _core.Version;
        _handle = new WaiterHandle(this, _core.Version);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static WaiterHandle Rent()
    {
        // ---- Thread-local fast path ----
        WaiterHandle? h = _tLocalPoolHead;
        if (h is not null)
        {
            _tLocalPoolHead = h.Next;
            _tLocalPoolCount--;
            h.Next = null;

            // Refresh handle for its waiter's current version / usage.
            // ResetForRent must clear per-use bits and Next.
            h.ResetForRent(h.Waiter._core.Version);
            return h;
        }

        // ---- Global lock-free pop ----
        while (true)
        {
            WaiterHandle? head = Volatile.Read(ref _sGlobalPoolHead);
            if (head is null)
            {
                // Allocate only when pool empty
                var w = new Waiter();
                return w._handle;
            }

            WaiterHandle? next = head.Next;
            if (Interlocked.CompareExchange(ref _sGlobalPoolHead, next, head) == head)
            {
                head.Next = null;
                head.ResetForRent(head.Waiter._core.Version);
                return head;
            }
        }
    }

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
                _core.SetException(new OperationCanceledException(cancellationToken));

            return;
        }

        _token = cancellationToken;
        _ctr = cancellationToken.UnsafeRegister(_cancelCallback, handle);
    }

    // Ensures that the waiter can only be completed once per version.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete(short version)
        => _state.TrySet((ushort)version | _completedBit, version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Cancel(short version)
    {
        if (!TryComplete(version))
            return;

        _core.SetException(new OperationCanceledException(_token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGrant(Releaser releaser, WaiterHandle handle)
    {
        if (!TryComplete(handle.Version))
            return false;

        handle.MarkGranted();
        _core.SetResult(releaser);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void TrySetException(Exception ex, short version)
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
            // Once the task is consumed, we can reset core fields…
            _ctr.Dispose();
            _ctr = default;
            _token = CancellationToken.None;

            _core.Reset();
            _state.Value = _core.Version;

            // ✅ DO NOT return the handle to the pool here.
            // It may still be sitting in the lock's queue until the head is popped.
            // Instead, mark "consumed" and let the dequeue-side recycle when safe.
            _handle.MarkConsumed();
        }
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    /// <summary>
    /// Called by WaiterHandle when both "consumed" and "dequeued" have happened.
    /// At this point the handle is no longer reachable from the lock queue and is safe to pool.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void RecycleHandle(WaiterHandle handle)
    {
        // Safe to wipe queue/pool linkage now
        handle.ResetForReturn();

        // Thread-local first
        if (_tLocalPoolCount < _maxLocal)
        {
            handle.Next = _tLocalPoolHead;
            _tLocalPoolHead = handle;
            _tLocalPoolCount++;
            return;
        }

        // Global lock-free push
        while (true)
        {
            WaiterHandle? head = Volatile.Read(ref _sGlobalPoolHead);
            handle.Next = head;

            if (Interlocked.CompareExchange(ref _sGlobalPoolHead, handle, head) == head)
                return;
        }
    }
}
