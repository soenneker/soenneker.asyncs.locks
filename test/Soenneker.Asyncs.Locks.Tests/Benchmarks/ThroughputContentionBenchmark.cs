using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

/// <summary>
/// 2) Many contenders, tiny critical section (throughput under contention).
/// Measures: scaling behavior + internal queueing overhead.
/// </summary>
[MemoryDiagnoser]
public class ThroughputContentionBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

    private Task[] _workers = Array.Empty<Task>();

    private int _counter;
    private ManualResetEventSlim _start = null!;

    [Params(1, 2, 4, 8, 16)]
    public int Contenders;

    [Params(100, 1000)]
    public int OpsPerWorker;

    [GlobalSetup]
    public void Setup()
    {
        _soennekerLock = new SoennekerAsyncLock();
        _nitoLock = new NitoAsyncLock();
        _nextensionsLock = new NExtensionsAsyncLock();
        _semaphoreSlim = new SemaphoreSlim(1, 1);

        _start = new ManualResetEventSlim(false);
        _workers = new Task[Contenders];
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _counter = 0;
        _start.Reset();

        if (_workers.Length != Contenders)
            _workers = new Task[Contenders];
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _soennekerLock.Dispose();
        _semaphoreSlim.Dispose();
        _start.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Soenneker: Throughput contention")]
    public async Task Soenneker()
    {
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                _start.Wait();
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    Releaser r = await _soennekerLock.Lock().ConfigureAwait(false);
                    try
                    {
                        _counter++;
                    }
                    finally
                    {
                        r.Dispose();
                    }
                }
            });
        }

        _start.Set();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter == 0) ThrowImpossible();
    }

    //[Benchmark(Description = "Nito: Throughput contention")]
    //public async Task Nito()
    //{
    //    for (int w = 0; w < Contenders; w++)
    //    {
    //        _workers[w] = Task.Run(async () =>
    //        {
    //            _start.Wait();
    //            for (int i = 0; i < OpsPerWorker; i++)
    //            {
    //                IDisposable r = await _nitoLock.LockAsync().ConfigureAwait(false);
    //                try
    //                {
    //                    _counter++;
    //                }
    //                finally
    //                {
    //                    r.Dispose();
    //                }
    //            }
    //        });
    //    }

    //    _start.Set();
    //    await Task.WhenAll(_workers).ConfigureAwait(false);

    //    if (_counter == 0) ThrowImpossible();
    //}

    //[Benchmark(Description = "NExtensions: Throughput contention")]
    //public async Task NExtensions()
    //{
    //    for (int w = 0; w < Contenders; w++)
    //    {
    //        _workers[w] = Task.Run(async () =>
    //        {
    //            _start.Wait();
    //            for (int i = 0; i < OpsPerWorker; i++)
    //            {
    //                var r = await _nextensionsLock.EnterScopeAsync().ConfigureAwait(false);
    //                try
    //                {
    //                    _counter++;
    //                }
    //                finally
    //                {
    //                    r.Dispose();
    //                }
    //            }
    //        });
    //    }

    //    _start.Set();
    //    await Task.WhenAll(_workers).ConfigureAwait(false);

    //    if (_counter == 0) ThrowImpossible();
    //}

    [Benchmark(Description = "SemaphoreSlim: Throughput contention")]
    public async Task SemaphoreSlim()
    {
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                _start.Wait();
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        _counter++;
                    }
                    finally
                    {
                        _semaphoreSlim.Release();
                    }
                }
            });
        }

        _start.Set();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter == 0) ThrowImpossible();
    }

    private static void ThrowImpossible() => throw new InvalidOperationException("Impossible");
}