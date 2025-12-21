using System;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks.Abstract;

/// <summary>
/// Represents a fast, safe lock that supports both async and synchronous use, optimized for low allocations and correct concurrency.
/// </summary>
/// <remarks>
/// <para>
/// This interface provides a lock mechanism optimized for low allocations and correct concurrency:
/// </para>
/// <list type="bullet">
/// <item><description>Internal gate: .NET 10 System.Threading.LockSync (protects queue + state transitions)</description></item>
/// <item><description>State tracking: ValueAtomicBool (_held, _disposed)</description></item>
/// <item><description>Async waits: pooled IValueTaskSource waiters (no Task alloc)</description></item>
/// <item><description>Sync waits: TaskCompletionSource only when contended (safe; avoids lost wakeups)</description></item>
/// <item><description>Dispose(): fails queued waiters + prevents new entrants (does not wait for current holder)</description></item>
/// <item><description>DisposeAsync(): Dispose() + waits until current holder (if any) exits</description></item>
/// </list>
/// </remarks>
public interface IAsyncLock : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Acquires the lock asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the lock acquisition.</param>
    /// <returns>
    /// A <see cref="ValueTask{T}"/> that completes when the lock is acquired, returning a <see cref="Releaser"/>
    /// that should be disposed to release the lock.
    /// </returns>
    /// <exception cref="ObjectDisposedException">Thrown when the lock has been disposed.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the cancellation token is canceled.</exception>
    ValueTask<Releaser> Lock(CancellationToken cancellationToken);

    ValueTask<Releaser> Lock();

    bool TryLock(out Releaser releaser);

    /// <summary>
    /// Acquires the lock synchronously (blocks the calling thread if contended).
    /// </summary>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the lock acquisition.</param>
    /// <returns>
    /// A <see cref="Releaser"/> that should be disposed to release the lock.
    /// </returns>
    /// <exception cref="ObjectDisposedException">Thrown when the lock has been disposed.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the cancellation token is canceled.</exception>
    Releaser LockSync(CancellationToken cancellationToken = default);
}