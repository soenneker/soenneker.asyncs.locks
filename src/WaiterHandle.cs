using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Atomics.ValueInts;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Represents a handle to a specific version of a waiter.
/// If the waiter has moved on to a new version, this class can
/// no longer be used to complete the waiter.
/// </summary>
internal sealed class WaiterHandle(Waiter waiter, short version)
{
    private const int _grantBit = 1;
    private const int _processedBit = 2;
    
    /// <summary>
    /// State encoding:
    /// - bit0: Grant bit (0 = not granted, 1 = lock granted)
    ///     The grant bit is set when this waiter receives ownership of the lock. It is used
    ///     to prevent the lock from decrementing the acquire count multiple times for a single
    ///     waiter, and thus losing a waiter. This could happen if the new lock owner calls
    ///     Exit() before the previous owner had finished the call to Exit().
    /// - bit1..2: Processed counter (Incremented when claimed in Lock() and processed in Exit())
    ///     The waiter must be claimed by a Lock method and also processed in the Exit()
    ///     function to be removed from the queue. The processed bits track when one of those has
    ///     happened so that the other knows it is time to pop the queue.
    /// </summary>
    private ValueAtomicInt _state;
    
    // Pointer for waiter queue (singly-linked list)
    public volatile WaiterHandle? Next;

    // The version allows this handle to become stale once the waiter task has been consumed
    public short Version { get; } = version;

    public bool IsGranted => (_state.Value & _grantBit) != 0;
    
    // Function called when a Lock() method claims the lock and when taking ownership
    // in the Exit() function. Will return true for the second caller, who takes on the
    // responsibility of popping the queue.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Process() => _state.Add(_processedBit) >= _processedBit * 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkGranted() => _state.Add(_grantBit);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser) => waiter.TryGrant(releaser, this);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Cancel() => waiter.Cancel(Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception e) => waiter.TrySetException(e, Version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask() => waiter.AsValueTask();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask(CancellationToken token) => waiter.AsValueTask(this, token);
}