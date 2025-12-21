[![](https://img.shields.io/nuget/v/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.locks/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.locks/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)

# Soenneker.Asyncs.Locks
### The fastest .NET async lock

This library provides a single primitive: `AsyncLock`.

### Design goal

The goal of `AsyncLock` is to provide the **fastest possible correct mutex** for real world .NET systems.

Specifically, it is designed to:

* make the uncontended case as close to a single atomic operation as possible
* avoid allocations, tasks, queues, and state machines unless contention actually occurs
* support cancellation and disposal without contaminating the fast path
* allow async and synchronous code to share the same lock safely
* preserve deterministic behavior under contention

---

## Installation

```bash
dotnet add package Soenneker.Asyncs.Locks
````

---

## Usage

### Async

```csharp
await using (await _lock.Lock(ct))
{
    // critical section
}
```

### Sync

```csharp
using (_lock.LockSync())
{
    // critical section
}
```

### Try-lock

```csharp
if (_lock.TryLock(out var releaser))
{
    using (releaser)
    {
        // critical section
    }
}
```

---

## Benchmarks

Async lock acquisition

| Method                  | Mean     | Median   | Allocated |
| ----------------------- | -------- | -------- | --------- |
| **Soenneker.AsyncLock** | 14.53 ns | 14.51 ns | 0 B       |
| SemaphoreSlim           | 20.68 ns | 20.09 ns | 0 B       |
| Nito.AsyncEx.AsyncLock  | 62.57 ns | 60.58 ns | 320 B     |

Synchronous lock acquisition

| Method                          | Mean      | Median    | Allocated |
|-------------------------------- |----------:|----------:|-----------|
| **Soenneker.AsyncLock (sync)**  |  8.08 ns  |  7.97 ns  | 0 B       |
| SemaphoreSlim (sync)            | 22.05 ns  | 21.76 ns  | 0 B       |
| Nito.AsyncEx.AsyncLock (sync)   | 60.27 ns  | 58.98 ns  | 320 B     |


### Correctness without compromise

* **Cancellation-aware**
  Supports cancellation before acquisition and while waiting for both async and sync callers.
  Cancelled waiters are removed immediately, never resumed, and never leaked with zero cost on the fast path.

* **Unified async and sync locking**
  A single mutex is shared by async and synchronous code paths. Ordering is preserved with no adapters, wrappers, or duplicate synchronization primitives.

* **Safe deterministic disposal**
  Disposal releases all waiters with `ObjectDisposedException`, can be awaited, and blocks until the current owner exits without penalizing uncontended performance.
