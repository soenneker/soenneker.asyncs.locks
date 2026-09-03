using Soenneker.Queues.Intrusive.Abstractions;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Soenneker.Asyncs.Locks;

internal sealed class Waiter : IValueTaskSource<Releaser>, IIntrusiveNode<Waiter>
{
    private const int _completedBit = 1 << 16;
    private const int _consumedBit = 1;
    private const int _dequeuedBit = 2;

    [ThreadStatic]
    private static Waiter? _localPool;

    private static readonly Action<object?> _cancelCallback = static state => ((Waiter)state!).Cancel();

    private int _state;
    private int _reclamationState;
    private ManualResetValueTaskSourceCore<Releaser> _core = new() {RunContinuationsAsynchronously = true};
    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private bool _cancellable;
    private short _queuedVersion;
    private Waiter? _next;

    private Waiter()
    {
    }

    public ref Waiter? Next
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref _next;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static Waiter Rent()
    {
        Waiter? waiter = _localPool;

        if (waiter is not null)
        {
            _localPool = waiter._next;
            waiter._next = null;
            return waiter;
        }

        return new Waiter();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask()
    {
        Prepare();
        return new ValueTask<Releaser>(this, _queuedVersion);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask(CancellationToken cancellationToken)
    {
        Prepare();

        if (cancellationToken.CanBeCanceled)
            RegisterCancellation(cancellationToken);

        return new ValueTask<Releaser>(this, _queuedVersion);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Prepare()
    {
        _cancellable = false;
        _queuedVersion = _core.Version;
        Volatile.Write(ref _state, (ushort)_queuedVersion);
    }

    private void RegisterCancellation(CancellationToken cancellationToken)
    {
        _cancellable = true;

        if (cancellationToken.IsCancellationRequested)
        {
            if (TryComplete())
                _core.SetException(new OperationCanceledException(cancellationToken));

            return;
        }

        _cancellationToken = cancellationToken;
        _registration = cancellationToken.UnsafeRegister(_cancelCallback, this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete()
        => Interlocked.CompareExchange(ref _state, (ushort)_queuedVersion | _completedBit, (ushort)_queuedVersion) == (ushort)_queuedVersion;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Cancel()
    {
        if (TryComplete())
            _core.SetException(new OperationCanceledException(_cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGrant(Releaser releaser)
    {
        if (!TryComplete())
            return false;

        _core.SetResult(releaser);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TrySetException(Exception exception)
    {
        if (!TryComplete())
            return false;

        _core.SetException(exception);
        return true;
    }

    public Releaser GetResult(short token)
    {
        Releaser result;

        try
        {
            result = _core.GetResult(token);
        }
        catch
        {
            ResetCancellation();
            _core.Reset();
            MarkConsumed();
            throw;
        }

        ResetCancellation();
        _core.Reset();
        Recycle(this);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ResetCancellation()
    {
        if (!_cancellable)
            return;

        _registration.Dispose();
        _registration = default;
        _cancellationToken = default;
        _cancellable = false;
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkConsumed()
    {
        if ((Interlocked.Or(ref _reclamationState, _consumedBit) & _dequeuedBit) != 0)
            Recycle(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkDequeued()
    {
        if ((Interlocked.Or(ref _reclamationState, _dequeuedBit) & _consumedBit) != 0)
            Recycle(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Recycle(Waiter waiter)
    {
        waiter._reclamationState = 0;
        waiter._next = _localPool;
        _localPool = waiter;
    }
}
