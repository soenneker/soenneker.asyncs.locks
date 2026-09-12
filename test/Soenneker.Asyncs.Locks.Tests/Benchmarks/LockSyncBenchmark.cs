using BenchmarkDotNet.Attributes;
using System;
using System.Threading;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockSyncBenchmark
{
    private readonly object _monitor = new();
    private readonly System.Threading.Lock _threadingLock = new();
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

    [GlobalSetup]
    public void Setup()
    {
        _soennekerLock = new SoennekerAsyncLock();
        _nitoLock = new NitoAsyncLock();
        _nextensionsLock = new NExtensionsAsyncLock();
        _semaphoreSlim = new SemaphoreSlim(1, 1);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _soennekerLock.Dispose();
        _semaphoreSlim.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Soenneker.AsyncLock (sync)")]
    public void SoennekerSync()
    {
        using Releaser releaser = _soennekerLock.LockSync();
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock (sync)")]
    public void NitoSync()
    {
        using IDisposable releaser = _nitoLock.Lock();
    }

    [Benchmark(Description = "NExtensions.Async.AsyncLock (sync)")]
    public void NExtensionsSync()
    {
        using NExtensionsAsyncLock.Releaser releaser = _nextensionsLock.EnterScopeAsync().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "SemaphoreSlim (sync)")]
    public void SemaphoreSlimSync()
    {
        _semaphoreSlim.Wait();
        _semaphoreSlim.Release();
    }

    [Benchmark(Description = "lock (object / Monitor)")]
    public void Monitor()
    {
        lock (_monitor)
        {

        }
    }
    [Benchmark(Description = "System.Threading.Lock")]
    public void ThreadingLock()
    {
        lock (_threadingLock)
        {

        }
    }
}
