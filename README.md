[![](https://img.shields.io/nuget/v/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.locks/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.locks/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.locks/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.locks/actions/workflows/codeql.yml)

# Soenneker.Asyncs.Locks

A low-allocation mutex shared by asynchronous and synchronous callers.

`AsyncLock` provides cancellable async acquisition, blocking synchronous acquisition, and a non-blocking try-lock. The uncontended path avoids allocating a `Task`; under contention, a direct first-waiter slot and immediately reclaimable intrusive MPSC queue hand off pooled `IValueTaskSource` waiters.

## Installation

```bash
dotnet add package Soenneker.Asyncs.Locks
```

## Asynchronous locking

Keep the lock on the object that owns the protected state and dispose every acquired `Releaser`:

```csharp
using Soenneker.Asyncs.Locks;

public sealed class BalanceStore : IAsyncDisposable
{
    private readonly AsyncLock _lock = new();
    private decimal _balance;

    public async ValueTask Add(
        decimal amount,
        CancellationToken cancellationToken)
    {
        using Releaser releaser = await _lock.Lock(cancellationToken);
        _balance += amount;
    }

    public ValueTask DisposeAsync() => _lock.DisposeAsync();
}
```

`Releaser` implements `IDisposable`, not `IAsyncDisposable`, so use `using` for the acquired token even inside an async method. `await using` is appropriate for the `AsyncLock` itself when the owner is disposed asynchronously.

The tokenless overload avoids cancellation registration when cancellation is not needed:

```csharp
using Releaser releaser = await asyncLock.Lock();
```

## Synchronous locking

Synchronous and asynchronous callers contend for the same mutex:

```csharp
using Releaser releaser = asyncLock.LockSync(cancellationToken);
// Protected synchronous work
```

`LockSync` blocks the calling thread while contended. Do not use it from asynchronous request paths merely to avoid `await`; use `Lock` there.

## Try without waiting

```csharp
if (asyncLock.TryLock(out Releaser releaser))
{
    using (releaser)
    {
        // The lock is held here.
    }
}
else
{
    // Another caller holds or is waiting for the lock, or it was disposed.
}
```

`TryLock` returns `false` instead of waiting. After disposal it also returns `false`; the blocking acquisition methods throw `ObjectDisposedException`.

## Cancellation

`Lock(CancellationToken)` and `LockSync(CancellationToken)` support cancellation before acquisition and while queued. A cancelled waiter does not enter the critical section. Once acquisition succeeds, cancellation does not release the lock; only disposing the returned `Releaser` does that.

Always keep acquisition outside the protected `try`/`finally` or `using` body so code does not attempt to release a lock it never acquired.

## Disposal

The two disposal methods intentionally differ:

| Method | Current holder | Queued and future callers |
| --- | --- | --- |
| `Dispose()` | May finish and release normally; disposal does not wait. | Queued callers fail and future blocking acquisitions throw. |
| `DisposeAsync()` | Waits for the current holder to release. | Queued callers fail and future blocking acquisitions throw. |

Neither method forcibly interrupts code already inside the critical section. Dispose the lock only when its owning service is shutting down and no new work should be accepted.

## Correctness rules

- `AsyncLock` is not reentrant. Code that already holds it must not acquire it again before releasing it.
- Keep critical sections short and avoid calling unknown code while holding the lock.
- Dispose each successful `Releaser` exactly once. It is a value type, so copying it and disposing multiple copies releases the mutex more than once and corrupts ownership state.
- Do not use the default value of `Releaser`; only use values returned by successful acquisition.
- A cancellation token controls acquisition only, not work performed after acquisition.

## API

| Member | Behavior |
| --- | --- |
| `Lock()` | Acquires asynchronously without cancellation registration. |
| `Lock(CancellationToken)` | Acquires asynchronously with cancellable waiting. |
| `LockSync(CancellationToken)` | Blocks until acquired or cancelled. |
| `TryLock(out Releaser)` | Attempts immediate acquisition. |
| `Dispose()` | Rejects waiters without waiting for the holder. |
| `DisposeAsync()` | Rejects waiters and waits for the holder to exit. |
