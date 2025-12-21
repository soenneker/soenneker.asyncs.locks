using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Soenneker.Asyncs.Locks.Tests;

[Collection("Collection")]
public sealed class AsyncLockTests
{
    [Fact]
    public async Task LockAsync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = await asyncLock.Lock();
        releaser.Dispose();
    }

    [Fact]
    public void LockSync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = asyncLock.LockSync();
        releaser.Dispose();
    }

    [Fact]
    public async Task LockAsync_Contended_WaitsForRelease()
    {
        using var asyncLock = new AsyncLock();
        var firstAcquired = false;
        var secondAcquired = false;

        // First task acquires lock
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            firstAcquired = true;
            await Task.Delay(50); // Hold lock briefly
        });

        // Wait for first to acquire
        await Task.Delay(10);
        Assert.True(firstAcquired);

        // Second task waits
        Task secondTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            secondAcquired = true;
        });

        // Second should not have acquired yet
        await Task.Delay(10);
        Assert.False(secondAcquired);

        // Wait for first to release
        await firstTask;
        
        // Now second should acquire
        await Task.Delay(10);
        Assert.True(secondAcquired);

        await secondTask;
    }

    [Fact]
    public void LockSync_Contended_WaitsForRelease()
    {
        using var asyncLock = new AsyncLock();
        var firstAcquired = false;
        var secondAcquired = false;

        // First thread acquires lock
        var firstThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync();
            firstAcquired = true;
            Thread.Sleep(50); // Hold lock briefly
        });

        firstThread.Start();
        Thread.Sleep(10); // Wait for first to acquire
        Assert.True(firstAcquired);

        // Second thread waits
        var secondThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync();
            secondAcquired = true;
        });

        secondThread.Start();
        Thread.Sleep(10); // Second should not have acquired yet
        Assert.False(secondAcquired);

        // Wait for first to release
        firstThread.Join();
        
        // Now second should acquire
        Thread.Sleep(10);
        Assert.True(secondAcquired);

        secondThread.Join();
    }

    [Fact]
    public async Task LockAsync_MultipleWaiters_ProcessesInOrder()
    {
        using var asyncLock = new AsyncLock();
        var order = new System.Collections.Concurrent.ConcurrentQueue<int>();

        // First acquires immediately
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            order.Enqueue(1);
            await Task.Delay(30);
        });

        await Task.Delay(5);

        // Second and third wait (start in deterministic order to avoid scheduling races)
        async Task Acquire(int id)
        {
            using Releaser releaser = await asyncLock.Lock();
            order.Enqueue(id);
        }

        Task secondTask = Acquire(2);
        Task thirdTask = Acquire(3);

        await firstTask;
        await Task.WhenAll(secondTask, thirdTask);

        int[] results = order.ToArray();
        Assert.Equal(new[] { 1, 2, 3 }, results);
    }

    [Fact]
    public async Task LockAsync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // First task holds the lock
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            await Task.Delay(100);
        });

        await Task.Delay(10);

        // Second task waits with cancellation
        Task<Releaser> secondTask = asyncLock.Lock(cts.Token)
                                             .AsTask();

        await Task.Delay(10);
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () => await secondTask);
        await firstTask;
    }

    [Fact]
    public void LockSync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // First thread holds the lock
        var firstThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync();
            Thread.Sleep(100);
        });

        firstThread.Start();
        Thread.Sleep(10);

        // Second thread waits with cancellation
        var canceled = false;
        var secondThread = new Thread(() =>
        {
            try
            {
                using Releaser releaser = asyncLock.LockSync(cts.Token);
                Assert.Fail("Should have been canceled");
            }
            catch (OperationCanceledException)
            {
                canceled = true;
            }
        });

        secondThread.Start();
        Thread.Sleep(10);
        cts.Cancel();
        secondThread.Join();

        Assert.True(canceled);
        firstThread.Join();
    }

    [Fact]
    public async Task LockAsync_AlreadyCanceled_ThrowsImmediately()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await asyncLock.Lock(cts.Token);
        });
    }

    [Fact]
    public void LockSync_AlreadyCanceled_ThrowsImmediately()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Throws<OperationCanceledException>(() =>
        {
            asyncLock.LockSync(cts.Token);
        });
    }

    [Fact]
    public async Task Dispose_PreventsNewAcquisitions()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await asyncLock.Lock();
        });

        Assert.Throws<ObjectDisposedException>(() =>
        {
            asyncLock.LockSync();
        });
    }

    [Fact]
    public async Task Dispose_FailsQueuedWaiters()
    {
        using var asyncLock = new AsyncLock();
        var firstAcquired = false;

        // First task holds the lock
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            firstAcquired = true;
            await Task.Delay(100);
        });

        await Task.Delay(10);
        Assert.True(firstAcquired);

        // Second task waits
        Task<Releaser> secondTask = asyncLock.Lock()
                                             .AsTask();

        await Task.Delay(10);
        asyncLock.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await secondTask);
        await firstTask;
    }

    [Fact]
    public async Task Dispose_AllowsCurrentHolderToComplete()
    {
        var asyncLock = new AsyncLock();
        var completed = false;

        Task task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            await Task.Delay(50);
            completed = true;
        });

        await Task.Delay(10);
        asyncLock.Dispose();
        await task;

        Assert.True(completed);
    }

    [Fact]
    public async Task DisposeAsync_WaitsForCurrentHolder()
    {
        var asyncLock = new AsyncLock();
        var released = false;

        Task task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            await Task.Delay(50);
            released = true;
        });

        await Task.Delay(10);
        ValueTask disposeTask = asyncLock.DisposeAsync();
        
        // DisposeAsync should wait
        await Task.Delay(30);
        Assert.False(disposeTask.IsCompleted);
        
        await task;
        await disposeTask;
        
        Assert.True(released);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();
        asyncLock.Dispose(); // Should not throw
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        await asyncLock.DisposeAsync();
        await asyncLock.DisposeAsync(); // Should not throw
    }

    [Fact]
    public async Task LockAsync_MixedWithLockSync_WorksCorrectly()
    {
        using var asyncLock = new AsyncLock();
        var order = new System.Collections.Concurrent.ConcurrentQueue<string>();

        // Async first
        Task asyncTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            order.Enqueue("async1");
            await Task.Delay(30);
        });

        await Task.Delay(5);

        // Sync waits
        var syncThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync();
            order.Enqueue("sync");
        });

        syncThread.Start();
        await Task.Delay(5);

        // Another async waits
        Task asyncTask2 = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            order.Enqueue("async2");
        });

        await asyncTask;
        syncThread.Join();
        await asyncTask2;

        string[] results = order.ToArray();
        Assert.Equal(new[] { "async1", "sync", "async2" }, results);
    }

    [Fact]
    public async Task Releaser_Dispose_ReleasesLock()
    {
        using var asyncLock = new AsyncLock();
        var secondAcquired = false;

        Task firstTask = Task.Run(async () =>
        {
            Releaser releaser = await asyncLock.Lock();
            await Task.Delay(30);
            releaser.Dispose(); // Explicit dispose
        });

        await Task.Delay(10);

        Task secondTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock();
            secondAcquired = true;
        });

        await firstTask;
        await Task.Delay(10);
        await secondTask;

        Assert.True(secondAcquired);
    }

    [Fact]
    public async Task LockAsync_RapidAcquireRelease_WorksCorrectly()
    {
        using var asyncLock = new AsyncLock();
        var count = 0;

        var tasks = new Task[10];
        for (int i = 0; i < 10; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using Releaser releaser = await asyncLock.Lock();
                    Interlocked.Increment(ref count);
                    await Task.Yield();
                }
            });
        }

        await Task.WhenAll(tasks);

        Assert.Equal(100, count);
    }

    [Fact]
    public void LockSync_RapidAcquireRelease_WorksCorrectly()
    {
        using var asyncLock = new AsyncLock();
        var count = 0;

        var threads = new Thread[10];
        for (int i = 0; i < 10; i++)
        {
            threads[i] = new Thread(() =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using Releaser releaser = asyncLock.LockSync();
                    Interlocked.Increment(ref count);
                }
            });
            threads[i].Start();
        }

        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        Assert.Equal(100, count);
    }

    [Fact]
    public async Task LockAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        var ex = await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await asyncLock.Lock();
        });

        Assert.Equal(nameof(AsyncLock), ex.ObjectName);
    }

    [Fact]
    public void LockSync_AfterDispose_ThrowsObjectDisposedException()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        var ex = Assert.Throws<ObjectDisposedException>(() =>
        {
            asyncLock.LockSync();
        });

        Assert.Equal(nameof(AsyncLock), ex.ObjectName);
    }

    [Fact]
    public async Task LockAsync_ConcurrentDispose_HandlesGracefully()
    {
        var asyncLock = new AsyncLock();
        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        // Multiple tasks trying to acquire
        var acquireTasks = new Task[5];
        for (int i = 0; i < 5; i++)
        {
            acquireTasks[i] = Task.Run(async () =>
            {
                try
                {
                    await asyncLock.Lock();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });
        }

        await Task.Delay(10);
        asyncLock.Dispose();

        await Task.WhenAll(acquireTasks);

        // All should have either succeeded before dispose or gotten ObjectDisposedException
        Assert.True(exceptions.All(e => e is ObjectDisposedException) || exceptions.Count == 0);
    }
}