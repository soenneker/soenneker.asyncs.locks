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

    /// <summary>
    /// Asynchronously acquires the lock and returns a releaser that releases the lock when disposed.
    /// </summary>
    /// <remarks>If the lock is already held, the returned task completes when the lock becomes available. The
    /// caller is responsible for disposing the returned <see cref="Releaser"/> to avoid deadlocks.</remarks>
    /// <returns>A <see cref="ValueTask{TResult}"/> that represents the asynchronous operation. The result contains a <see
    /// cref="Releaser"/> that must be disposed to release the lock.</returns>
    ValueTask<Releaser> Lock();

    /// <summary>
    /// Attempts to acquire the lock without blocking.
    /// </summary>
    /// <remarks>Use the returned <see cref="Releaser"/> in a using statement to ensure the lock is properly
    /// released. If the method returns false, the caller does not own the lock and must not attempt to release
    /// it.</remarks>
    /// <param name="releaser">When this method returns, contains a <see cref="Releaser"/> that can be used to release the lock if the
    /// operation succeeds; otherwise, contains a default value.</param>
    /// <returns>true if the lock was successfully acquired; otherwise, false.</returns>
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