using BenchmarkDotNet.Attributes;
using System;
using System.Threading;
using System.Threading.Tasks;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

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

    [Benchmark(Baseline = true, Description = "Soenneker.Asyncs.Lock")]
    public async ValueTask SoennekerAsync()
    {
        using Releaser releaser = await _soennekerLock.Lock();
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock")]
    public async ValueTask NitoAsync()
    {
        using IDisposable releaser = await _nitoLock.LockAsync();
    }

    [Benchmark(Description = "SemaphoreSlim")]
    public async ValueTask SemaphoreSlimAsync()
    {
        await _semaphoreSlim.WaitAsync();
        _semaphoreSlim.Release();
    }
}