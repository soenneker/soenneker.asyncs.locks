using BenchmarkDotNet.Attributes;
using System;
using System.Threading;
using System.Threading.Tasks;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockBenchmark
{
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

    [Benchmark(Baseline = true, Description = "Soenneker.Asyncs.Lock")]
    public async ValueTask SoennekerAsync()
    {
        using Releaser releaser = await _soennekerLock.Lock().ConfigureAwait(false);
    }

    //[Benchmark(Description = "Nito.AsyncEx.AsyncLock")]
    //public async ValueTask NitoAsync()
    //{
    //    using IDisposable releaser = await _nitoLock.LockAsync().ConfigureAwait(false);
    //}

    //[Benchmark(Description = "NExtensions.Async.AsyncLock")]
    //public async ValueTask NExtensionsAsync()
    //{
    //    using NExtensionsAsyncLock.Releaser releaser = await _nextensionsLock.EnterScopeAsync().ConfigureAwait(false);
    //}

    [Benchmark(Description = "SemaphoreSlim")]
    public async ValueTask SemaphoreSlimAsync()
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        _semaphoreSlim.Release();
    }
}