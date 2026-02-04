using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Soenneker.Asyncs.Locks.Tests.Enums;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

/// <summary>
/// 3) Hold-time contention: measures behavior when the lock is held long enough for waiters to build.
/// Choose hold mode: SpinWait (CPU hold), Yield (async boundary hold), or Delay (I/O-like hold).
/// </summary>
[MemoryDiagnoser]
public class HoldTimeContentionBenchmark
{
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

    private Task[] _workers = Array.Empty<Task>();
    private ManualResetEventSlim _start = null!;

    private int _counter;

    [Params(2, 4, 8, 16)]
    public int Contenders;

    [Params(100)]
    public int OpsPerWorker;

    [Params(HoldMode.SpinWait, HoldMode.Yield, HoldMode.Delay)]
    public HoldMode Hold;

    // SpinWait iterations or Delay milliseconds, depending on Hold.
    [Params(50, 200)]
    public int HoldAmount;

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

    [Benchmark(Baseline = true, Description = "Soenneker: Hold-time contention")]
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
                        await DoHoldAsync().ConfigureAwait(false);
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

    [Benchmark(Description = "Nito: Hold-time contention")]
    public async Task Nito()
    {
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                _start.Wait();
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    IDisposable r = await _nitoLock.LockAsync().ConfigureAwait(false);
                    try
                    {
                        await DoHoldAsync().ConfigureAwait(false);
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

    //[Benchmark(Description = "NExtensions: Hold-time contention")]
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
    //                    await DoHoldAsync().ConfigureAwait(false);
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

    [Benchmark(Description = "SemaphoreSlim: Hold-time contention")]
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
                        await DoHoldAsync().ConfigureAwait(false);
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

    private async ValueTask DoHoldAsync()
    {
        switch (Hold)
        {
            case HoldMode.None:
                return;

            case HoldMode.SpinWait:
                Thread.SpinWait(HoldAmount);
                return;

            case HoldMode.Yield:
                await Task.Yield();
                return;

            case HoldMode.Delay:
                await Task.Delay(HoldAmount);
                return;
        }
    }

    private static void ThrowImpossible() => throw new InvalidOperationException("Impossible");
}