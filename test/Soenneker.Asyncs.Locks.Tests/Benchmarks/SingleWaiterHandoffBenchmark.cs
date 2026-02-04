using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

/// <summary>
/// 1) Single-waiter enqueue + handoff cost (your current idea), but looped to reduce harness noise.
/// Measures: acquire fast-path + enqueue 1 waiter + release + waiter resume.
/// </summary>
public class SingleWaiterHandoffBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

    [Params(1)]
    //[Params(1, 10, 100, 1_000)]
    public int Ops;

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

    [Benchmark(Baseline = true, Description = "Soenneker: Single waiter handoff")]
    public async ValueTask Soenneker()
    {
        for (int i = 0; i < Ops; i++)
        {
            Releaser releaser = await _soennekerLock.Lock().ConfigureAwait(false);
            ValueTask<Releaser> contend = _soennekerLock.Lock();
            releaser.Dispose();
            Releaser releaser2 = await contend.ConfigureAwait(false);
            releaser2.Dispose();
        }
    }

    [Benchmark(Description = "SemaphoreSlim: Single waiter handoff")]
    public async ValueTask SemaphoreSlim()
    {
        for (int i = 0; i < Ops; i++)
        {
            await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            Task contend = _semaphoreSlim.WaitAsync();
            _semaphoreSlim.Release();
            await contend.ConfigureAwait(false);
            _semaphoreSlim.Release();
        }
    }

    //[Benchmark(Description = "Nito: Single waiter handoff")]
    //public async ValueTask Nito()
    //{
    //    for (int i = 0; i < Ops; i++)
    //    {
    //        IDisposable releaser = await _nitoLock.LockAsync().ConfigureAwait(false);
    //        var contend = _nitoLock.LockAsync();
    //        releaser.Dispose();
    //        var releaser2 = await contend.ConfigureAwait(false);
    //        releaser2.Dispose();
    //    }
    //}

    //[Benchmark(Description = "NExtensions: Single waiter handoff")]
    //public async ValueTask NExtensions()
    //{
    //    for (int i = 0; i < Ops; i++)
    //    {
    //        NExtensionsAsyncLock.Releaser releaser = await _nextensionsLock.EnterScopeAsync().ConfigureAwait(false);
    //        ValueTask<NExtensionsAsyncLock.Releaser> contend = _nextensionsLock.EnterScopeAsync();
    //        releaser.Dispose();
    //        var releaser2 = await contend.ConfigureAwait(false);
    //        releaser2.Dispose();
    //    }
    //}
}