using BenchmarkDotNet.Attributes;
using System;
using System.Threading;
using System.Threading.Tasks;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockOverWorkBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;
    private int _counter;

    [GlobalSetup]
    public void Setup()
    {
        _soennekerLock = new SoennekerAsyncLock();
        _nitoLock = new NitoAsyncLock();
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
        using Releaser releaser = await _soennekerLock.Lock().ConfigureAwait(false);
        _counter++;
        Thread.SpinWait(16);
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock + small work")]
    public async ValueTask NitoAsync_WithWork()
    {
        using IDisposable releaser = await _nitoLock.LockAsync().ConfigureAwait(false);
        _counter++;
        Thread.SpinWait(16);
    }

    [Benchmark(Description = "SemaphoreSlim + small work")]
    public async ValueTask SemaphoreSlimAsync_WithWork()
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        _counter++;
        Thread.SpinWait(16);
        _semaphoreSlim.Release();
    }
}