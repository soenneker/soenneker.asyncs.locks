using System;
using System.Runtime.CompilerServices;

namespace Soenneker.Asyncs.Locks;

/// <summary>Token that releases the lock when disposed.</summary>
public readonly struct Releaser : IDisposable
{
    private readonly AsyncLock? _owner;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Releaser(AsyncLock owner) => _owner = owner;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _owner?.Exit();
}