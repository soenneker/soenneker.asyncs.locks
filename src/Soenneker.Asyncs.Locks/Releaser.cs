using System;
using System.Runtime.CompilerServices;

namespace Soenneker.Asyncs.Locks;

/// <summary>Token that releases the lock when disposed.</summary>
public readonly struct Releaser(AsyncLock owner) : IDisposable
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => owner.Exit();
}