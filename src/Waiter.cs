using Soenneker.Atomics.ValueInts;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Wait node:
/// - async/sync mode: TaskCompletionSource (pooled waiter, new TCS per wait)
/// </summary>
internal sealed class Waiter
{
    private static readonly ConcurrentBag<Waiter> _pool = [];

    private TaskCompletionSource<Releaser>? _tcs;

    private CancellationToken _token;
    private CancellationTokenRegistration _ctr;

    // completion state:
    // 0 = pending, 1 = canceled, 2 = signaled (result/exception)
    private ValueAtomicInt _completion;

    // lifecycle bit flags:
    // 1 = dequeued from AsyncLock queue
    // 2 = consumed by waiter (awaited or sync waited)
    private ValueAtomicInt _lifecycle;

    private Waiter()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Waiter Rent()
    {
        if (!_pool.TryTake(out Waiter? w))
            w = new Waiter();

        w._tcs = new TaskCompletionSource<Releaser>(TaskCreationOptions.RunContinuationsAsynchronously);
        w._token = CancellationToken.None;
        w._ctr = default;

        w._lifecycle.Value = 0;
        w._completion.Value = 0;

        w._tcs.Task.ContinueWith(
            static (_, state) => ((Waiter)state!).MarkConsumed(),
            w,
            CancellationToken.None,
            TaskContinuationOptions.RunContinuationsAsynchronously,
            TaskScheduler.Default);

        return w;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Waiter RentSync()
    {
        if (!_pool.TryTake(out Waiter? w))
            w = new Waiter();

        w._tcs = new TaskCompletionSource<Releaser>(TaskCreationOptions.RunContinuationsAsynchronously);
        w._token = CancellationToken.None;
        w._ctr = default;

        w._lifecycle.Value = 0;
        w._completion.Value = 0;

        return w;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<Releaser> AsValueTask() => new(_tcs!.Task);

    /// <summary>
    /// Called by <see cref="AsyncLock"/> after this waiter is removed from its internal queue.
    /// Returns false if the waiter was already consumed (e.g., cancellation observed) and must not be touched further.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MarkDequeued()
    {
        int prev = _lifecycle.Or(1);

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
        int prev = _lifecycle.Or(2);

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
        var tcs = _tcs;

        if (tcs is null)
            return;

        if (_completion.CompareExchange(1, 0) != 0)
            return;

        var token = _token;

        // Don't dispose or clear _ctr here (callback thread).
        tcs.TrySetException(new OperationCanceledException(token));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser)
    {
        var tcs = _tcs;

        if (tcs is null)
            return false;

        if (_completion.CompareExchange(2, 0) != 0)
            return false;

        try
        {
            return tcs.TrySetResult(releaser);
        }
        finally
        {
            CleanupCancellation();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception ex)
    {
        var tcs = _tcs;
        if (tcs is null)
            return;

        if (_completion.CompareExchange(2, 0) != 0)
            return;

        try
        {
            tcs.TrySetException(ex);
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
        // Safe here; Return happens after completion is observed.
        if (_ctr != default)
            _ctr.Dispose();

        _ctr = default;
        _token = CancellationToken.None;

        _tcs = null;
        _lifecycle.Value = 0;
        _completion.Value = 0;
        _pool.Add(this);
    }
}
