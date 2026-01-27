using BenchmarkDotNet.Attributes;
using Nito.AsyncEx;
using System;
using System.Threading;
using System.Threading.Tasks;
using NExtensionsAsyncLock = NExtensions.Async.AsyncLock;
using NitoAsyncLock = Nito.AsyncEx.AsyncLock;
using SoennekerAsyncLock = Soenneker.Asyncs.Locks.AsyncLock;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

[MemoryDiagnoser]
public class LockContendBenchmark
{
    // NOTE: I'm not even sure if this is a valuable benchmark to have
    
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
        Releaser releaser = await _soennekerLock.Lock().ConfigureAwait(false);
        ValueTask<Releaser> contend = _soennekerLock.Lock();
        releaser.Dispose();
        var releaser2 = await contend.ConfigureAwait(false);
        releaser2.Dispose();
    }

    [Benchmark(Description = "Nito.AsyncEx.AsyncLock")]
    public async ValueTask NitoAsync()
    {
        IDisposable releaser = await _nitoLock.LockAsync().ConfigureAwait(false);
        AwaitableDisposable<IDisposable> contend = _nitoLock.LockAsync();
        releaser.Dispose();
        var releaser2 = await contend.ConfigureAwait(false);
        releaser2.Dispose();
    }

    [Benchmark(Description = "NExtensions.Async.AsyncLock")]
    public async ValueTask NExtensionsAsync()
    {
        NExtensionsAsyncLock.Releaser releaser = await _nextensionsLock.EnterScopeAsync().ConfigureAwait(false);
        ValueTask<NExtensionsAsyncLock.Releaser> contend = _nextensionsLock.EnterScopeAsync();
        releaser.Dispose();
        var releaser2 = await contend.ConfigureAwait(false);
        releaser2.Dispose();
    }

    [Benchmark(Description = "SemaphoreSlim")]
    public async ValueTask SemaphoreSlimAsync()
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        Task contend = _semaphoreSlim.WaitAsync();
        _semaphoreSlim.Release();
        await contend.ConfigureAwait(false);
        _semaphoreSlim.Release();
    }
}