using BenchmarkDotNet.Attributes;
using System;
using System.Threading;
using System.Threading.Tasks;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockOverWorkBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;
    private int _counter;

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

    [Benchmark(Description = "Soenneker.AsyncLock (async) + small work")]
    public async ValueTask SoennekerAsync_WithWork()
    {
        using (await _soennekerLock.Lock())
        {
            _counter++;
            Thread.SpinWait(16);
        }
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock + small work")]
    public async ValueTask NitoAsync_WithWork()
    {
        using (await _nitoLock.LockAsync())
        {
            _counter++;
            Thread.SpinWait(16);
        }
    }

    [Benchmark(Description = "NExtensions.Async.AsyncLock + small work")]
    public async ValueTask NExtensionsAsync_WithWork()
    {
        using (await _nextensionsLock.EnterScopeAsync())
        {
            _counter++;
            Thread.SpinWait(16);
        }
    }

    [Benchmark(Description = "SemaphoreSlim + small work")]
    public async ValueTask SemaphoreSlimAsync_WithWork()
    {
        await _semaphoreSlim.WaitAsync();
        _counter++;
        Thread.SpinWait(16);
        _semaphoreSlim.Release();
    }
}