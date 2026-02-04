using BenchmarkDotNet.Attributes;
using System.Threading;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockSyncOverWorkBenchmark
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

    [Benchmark(Description = "Soenneker.AsyncLock (sync) + small work")]
    public void SoennekerSync_WithWork()
    {
        using (_soennekerLock.LockSync())
        {
            _counter++;
            Thread.SpinWait(16); // tiny critical-section work
        }
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock (sync) + small work")]
    public void NitoSync_WithWork()
    {
        using (_nitoLock.Lock())
        {
            _counter++;
            Thread.SpinWait(16);
        }
    }

    [Benchmark(Description = "NExtensions.Async.AsyncLock (sync) + small work")]
    public void NExtensionsSync_WithWork()
    {
        using (_nextensionsLock.EnterScopeAsync().GetAwaiter().GetResult())
        {
            _counter++;
            Thread.SpinWait(16);
        }
    }

    [Benchmark(Description = "SemaphoreSlim (sync) + small work")]
    public void SemaphoreSlimSync_WithWork()
    {
        _semaphoreSlim.Wait();
        _counter++;
        Thread.SpinWait(16);
        _semaphoreSlim.Release();
    }
}