using Soenneker.Atomics.ValueInts;
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

    private ValueAtomicInt _state;
    private ValueAtomicInt _reclamationState;
    private ManualResetValueTaskSourceCore<Releaser> _core = new() {RunContinuationsAsynchronously = true};
    private CancellationToken _cancellationToken;
    private CancellationTokenRegistration _registration;
    private bool _cancellable;
    private bool _requiresArbitration;
    // -1: async; 0: synchronous spinner; 1: monitor waiter; 2: sync completion published.
    private int _syncState;
    private short _queuedVersion;
    private Waiter? _next;

    private Waiter()
    {
    }

    internal bool CanGrantDirectly
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => !_requiresArbitration;
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
        _requiresArbitration = false;
        _syncState = -1;
        _queuedVersion = _core.Version;
        _state.VolatileWrite((ushort)_queuedVersion);
    }

    internal void PrepareSync(CancellationToken cancellationToken)
    {
        Prepare();
        _syncState = 0;
        // Thread interruption can compete with a grant even without a cancellation token.
        _requiresArbitration = true;
        if (cancellationToken.CanBeCanceled)
            RegisterCancellation(cancellationToken);
    }

    internal Releaser GetResultSync()
    {
        ThreadInterruptedException? interrupted = null;
        short version = _queuedVersion;
        var spinner = new SpinWait();
        while (Volatile.Read(ref _syncState) != 2 && !spinner.NextSpinWillYield)
            spinner.SpinOnce();

        while (Volatile.Read(ref _syncState) != 2)
        {
            try
            {
                lock (this)
                {
                    if (interrupted is not null)
                        TrySetException(interrupted);

                    // Register the need for a pulse while holding the monitor so a
                    // completion cannot slip between the predicate and Wait.
                    Interlocked.CompareExchange(ref _syncState, 1, 0);
                    while (Volatile.Read(ref _syncState) != 2)
                        Monitor.Wait(this);
                }

                break;
            }
            catch (ThreadInterruptedException exception)
            {
                // Entering the monitor can also be interrupted. Reserve the failure
                // after reacquiring it so completion cannot be abandoned midway.
                interrupted = exception;
            }
        }

        // Consume outside the monitor: disposing a cancellation registration may
        // wait for a callback that needs this monitor to finish its notification.
        Releaser result = GetResult(version);
        if (interrupted is not null)
        {
            result.Dispose();
            throw interrupted;
        }

        return result;
    }

    private void RegisterCancellation(CancellationToken cancellationToken)
    {
        _cancellable = true;
        _requiresArbitration = true;

        if (cancellationToken.IsCancellationRequested)
        {
            if (TryComplete())
                CompleteException(new OperationCanceledException(cancellationToken));

            return;
        }

        _cancellationToken = cancellationToken;
        _registration = cancellationToken.UnsafeRegister(static state => ((Waiter)state!).Cancel(), this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryComplete()
        => _state.CompareExchange((ushort)_queuedVersion | _completedBit, (ushort)_queuedVersion) == (ushort)_queuedVersion;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Cancel()
    {
        if (TryComplete())
            CompleteException(new OperationCanceledException(_cancellationToken));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryReserveGrant() => TryComplete();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void CompleteGrant(Releaser releaser)
    {
        if (_syncState >= 0)
            CompleteSync(releaser);
        else
            _core.SetResult(releaser);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void CompleteSync(Releaser releaser)
    {
        _core.SetResult(releaser);
        NotifySync();
    }

    private void CompleteException(Exception exception)
    {
        if (_syncState < 0)
        {
            _core.SetException(exception);
            return;
        }

        _core.SetException(exception);
        NotifySync();
    }

    private void NotifySync()
    {
        // This is the last access to completion state: a spinning consumer may
        // immediately consume and recycle the waiter after observing 2.
        if (Interlocked.Exchange(ref _syncState, 2) == 1)
            PulseSync();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void PulseSync()
    {
        bool interrupted = false;
        while (true)
        {
            try
            {
                lock (this)
                    Monitor.Pulse(this);
                break;
            }
            catch (ThreadInterruptedException)
            {
                // A producer must finish notification even if interrupted while
                // entering the monitor; restore the pending interrupt afterward.
                interrupted = true;
            }
        }

        // A delayed pulse can reach a reused waiter. It is harmless because every
        // monitor wait checks its own completion predicate before proceeding.
        if (interrupted)
            Thread.CurrentThread.Interrupt();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TrySetException(Exception exception)
    {
        if (!TryComplete())
            return false;

        CompleteException(exception);
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
        if ((_reclamationState.Or(_consumedBit) & _dequeuedBit) != 0)
            Recycle(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void MarkDequeued()
    {
        if ((_reclamationState.Or(_dequeuedBit) & _consumedBit) != 0)
            Recycle(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Recycle(Waiter waiter)
    {
        waiter._reclamationState = default;
        waiter._next = _localPool;
        _localPool = waiter;
    }
}
