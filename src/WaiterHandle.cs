using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks;

/// <summary>
/// Represents a handle to a specific version of a waiter.
/// If the waiter has moved on to a new version, this class can
/// no longer be used to complete the waiter.
/// </summary>
internal sealed class WaiterHandle(Waiter waiter, short version)
{
    // The waiter must be handed out in a Lock method and also processed in the Exit()
    // function to be removed from the queue. This bool tracks when one of those has
    // happened, so that the other knows it is time to pop the queue.
    public bool Processed;
    
    // Pointer for waiter queue (singly-linked list)
    public WaiterHandle? Next;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGrant(Releaser releaser) => waiter.TryGrant(releaser, version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Cancel() => waiter.Cancel(version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void TrySetException(Exception e) => waiter.TrySetException(e, version);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask() => waiter.AsValueTask();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<Releaser> NewValueTask(CancellationToken token) => waiter.AsValueTask(this, token);
}