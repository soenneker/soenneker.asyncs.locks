using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Atomics.ValueInts;
using Soenneker.Queues.Intrusive.Abstractions;

namespace Soenneker.Asyncs.Locks;

internal sealed class WaiterHandle : IIntrusiveNode<WaiterHandle>
{
    private const int _grantBit = 1;

    // recycle handshake bits
    private const int _consumedBit = 8;
    private const int _dequeuedBit = 16;

    private ValueAtomicInt _state;

    // Used for BOTH queue linkage and pool linkage.
    // Must be stable storage because IntrusiveMpscQueue uses Volatile.Read/Write(ref Next).
    private WaiterHandle? _next;

    /// <summary>
    /// Gets a reference to the next link storage used by the intrusive queue (and optionally the pool).
    /// </summary>
    public ref WaiterHandle? Next
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref _next;
    }

    public Waiter Waiter { get; }

    public short Version { get; private set; }

    public WaiterHandle(Waiter waiter, short version)
    {
        Waiter = waiter;
        Version = version;
        _state.Value = 0;
        _next = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ResetForRent(short version)
    {
        Version = version;
        _state.Value = 0;   // clear all bits
        _next = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ResetForReturn()
    {
        _state.Value = 0;
        _next = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkGranted()
        => _state.Or(_grantBit);

    /// <summary>
    /// Called by Waiter.GetResult() after the continuation consumes the result.
    /// If already dequeued, recycle immediately.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkConsumed()
    {
        if ((_state.Or(_consumedBit) & _dequeuedBit) != 0)
            Waiter.RecycleHandle(this);
    }

    /// <summary>
    /// Called by AsyncLock when the handle is removed from the queue (head advanced).
    /// If already consumed, recycle immediately.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkDequeued()
    {
        if ((_state.Or(_dequeuedBit) & _consumedBit) != 0)
            Waiter.RecycleHandle(this);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser)
        => Waiter.TryGrant(releaser, this);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Cancel()
        => Waiter.Cancel(Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception e)
        => Waiter.TrySetException(e, Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask()
        => Waiter.AsValueTask();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask(CancellationToken token)
        => Waiter.AsValueTask(this, token);
}
