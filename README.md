[![](https://img.shields.io/nuget/v/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.locks/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.locks/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.asyncs.locks.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.locks/)

# Soenneker.Asyncs.Locks
### The fastest .NET async lock

This library provides a single primitive: `AsyncLock`.

### Design goals & guarantees

`AsyncLock` is built to be the **fastest possible correct mutex** for real-world .NET systems.

It provides the following guarantees:

#### Correctness (always)

* **Cancellation-safe**
  Fully supports cancellation before acquisition and while waiting for both async and sync callers.  
  Cancelled waiters are removed immediately, never resumed, and never leaked — with **zero impact on the fast path**.

* **Unified async + sync locking**
  Async and synchronous callers share the *same mutex*.  
  Ordering is preserved without adapters, wrappers, or duplicated synchronization primitives.

#### Performance (by design)

* Uncontended acquisition is as close to a single atomic operation as possible
* No allocations, tasks, or state machines unless contention occurs
* Cancellation and disposal logic are completely excluded from the fast path
* Deterministic behavior under contention

---

## Installation

```bash
dotnet add package Soenneker.Asyncs.Locks
```

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

| Method                      | Mean     | Error    | StdDev   | Ratio        | RatioSD | Gen0   | Allocated | 
|---------------------------- |---------:|---------:|---------:|-------------:|--------:|-------:|----------:|
| **Soenneker.Asyncs.Lock**       | 10.06 ns | 0.212 ns | 0.393 ns |     baseline |         |      - |         - |
| SemaphoreSlim               | 19.17 ns | 0.406 ns | 0.360 ns | 1.91x slower |   0.08x |      - |         - |
| Nito.AsyncEx.AsyncLock      | 55.32 ns | 1.078 ns | 2.645 ns | 5.51x slower |   0.33x | 0.0191 |     320 B |

Synchronous lock acquisition

| Method                               | Mean      | Error     | StdDev    | Median    | Ratio        | RatioSD | Gen0   | Allocated |
|------------------------------------- |----------:|----------:|----------:|----------:|-------------:|--------:|-------:|----------:|
| **'Soenneker.AsyncLock (sync)'**         |  8.087 ns | 0.1792 ns | 0.3138 ns |  8.025 ns |     baseline |         |      - |         - |
| 'SemaphoreSlim (sync)'               | 19.494 ns | 0.4031 ns | 0.7268 ns | 19.091 ns | 2.41x slower |   0.13x |      - |         - |
| 'Nito.AsyncEx.AsyncLock (sync)'      | 48.427 ns | 1.0048 ns | 2.9150 ns | 48.046 ns | 6.00x slower |   0.43x | 0.0191 |     320 B |
