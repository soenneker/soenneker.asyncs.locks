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
    private readonly object _monitor = new();
    private readonly System.Threading.Lock _threadingLock = new();
    private SoennekerAsyncLock _soennekerLock = null!;
    private NitoAsyncLock _nitoLock = null!;
    private NExtensionsAsyncLock _nextensionsLock = null!;
    private SemaphoreSlim _semaphoreSlim = null!;

    private Task[] _workers = Array.Empty<Task>();

    private int _counter;
    private TaskCompletionSource _start = null!;

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

        _start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _workers = new Task[Contenders];
    }

    // Reset per invocation, including warmup and pilot invocations.
    private void ResetBatch()
    {
        _counter = 0;
        _start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (_workers.Length != Contenders)
            _workers = new Task[Contenders];
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _soennekerLock.Dispose();
        _semaphoreSlim.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Soenneker: Throughput contention")]
    public async Task Soenneker()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
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

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }

    [Benchmark(Description = "Nito: Throughput contention")]
    public async Task Nito()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    IDisposable r = await _nitoLock.LockAsync().ConfigureAwait(false);
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

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }

    [Benchmark(Description = "NExtensions: Throughput contention")]
    public async Task NExtensions()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    var r = await _nextensionsLock.EnterScopeAsync().ConfigureAwait(false);
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

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }

    [Benchmark(Description = "SemaphoreSlim: Throughput contention")]
    public async Task SemaphoreSlim()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
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

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);

        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }

    private static void ThrowImpossible() => throw new InvalidOperationException("Protected increment count does not match completed operations.");

    [Benchmark(Description = "lock (object / Monitor) : Throughput contention")]
    public async Task Monitor()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    lock (_monitor)
                    {
                        _counter++;
                    }
                }
            });
        }

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);
        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }
    [Benchmark(Description = "System.Threading.Lock : Throughput contention")]
    public async Task ThreadingLock()
    {
        ResetBatch();
        for (int w = 0; w < Contenders; w++)
        {
            _workers[w] = Task.Run(async () =>
            {
                await _start.Task.ConfigureAwait(false);
                for (int i = 0; i < OpsPerWorker; i++)
                {
                    lock (_threadingLock)
                    {
                        _counter++;
                    }
                }
            });
        }

        _start.SetResult();
        await Task.WhenAll(_workers).ConfigureAwait(false);
        if (_counter != Contenders * OpsPerWorker) ThrowImpossible();
    }
}
