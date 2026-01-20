using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Xunit;

namespace Soenneker.Asyncs.Locks.Tests;

[Collection("Collection")]
public sealed class AsyncLockTests
{
    [Fact]
    public async Task LockAsync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
        releaser.Dispose();
    }

    [Fact]
    public void LockSync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = asyncLock.LockSync(TestContext.Current.CancellationToken);
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
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            firstAcquired = true;
            await Task.Delay(50, TestContext.Current.CancellationToken); // Hold lock briefly
        }, TestContext.Current.CancellationToken);

        // Wait for first to acquire
        await Task.Delay(10, TestContext.Current.CancellationToken);
        firstAcquired.Should().BeTrue();

        // Second task waits
        Task secondTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            secondAcquired = true;
        }, TestContext.Current.CancellationToken);

        // Second should not have acquired yet
        await Task.Delay(10, TestContext.Current.CancellationToken);
        secondAcquired.Should().BeFalse();

        // Wait for first to release
        await firstTask;
        
        // Now second should acquire
        await Task.Delay(10, TestContext.Current.CancellationToken);
        secondAcquired.Should().BeTrue();

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
            using Releaser releaser = asyncLock.LockSync(TestContext.Current.CancellationToken);
            firstAcquired = true;
            Thread.Sleep(50); // Hold lock briefly
        });

        firstThread.Start();
        Thread.Sleep(10); // Wait for first to acquire
        firstAcquired.Should().BeTrue();

        // Second thread waits
        var secondThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync(TestContext.Current.CancellationToken);
            secondAcquired = true;
        });

        secondThread.Start();
        Thread.Sleep(10); // Second should not have acquired yet
        secondAcquired.Should().BeFalse();

        // Wait for first to release
        firstThread.Join();
        
        // Now second should acquire
        Thread.Sleep(10);
        secondAcquired.Should().BeTrue();

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
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            order.Enqueue(1);
            await Task.Delay(30, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        // Second and third wait (start in deterministic order to avoid scheduling races)
        async Task Acquire(int id)
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            order.Enqueue(id);
        }

        Task secondTask = Acquire(2);
        Task thirdTask = Acquire(3);

        await firstTask;
        await Task.WhenAll(secondTask, thirdTask);

        int[] results = order.ToArray();
        results.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task LockAsync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // First task holds the lock
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);

        // Second task waits with cancellation
        Task<Releaser> secondTask = asyncLock.Lock(cts.Token)
                                             .AsTask();

        await Task.Delay(10, TestContext.Current.CancellationToken);
        cts.Cancel();

        Func<Task> act = async () => await secondTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
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
                throw new Exception("Should have been canceled");
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

        canceled.Should().BeTrue();
        firstThread.Join();
    }

    [Fact]
    public async Task LockAsync_AlreadyCanceled_ThrowsImmediately()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = asyncLock.Awaiting(l => l.Lock(cts.Token).AsTask());
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void LockSync_AlreadyCanceled_ThrowsImmediately()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        asyncLock.Invoking(l => l.LockSync(cts.Token))
                 .Should()
                 .Throw<OperationCanceledException>();
    }

    [Fact]
    public async Task Dispose_PreventsNewAcquisitions()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        var act = asyncLock.Awaiting(l => l.Lock().AsTask());
        await act.Should().ThrowAsync<ObjectDisposedException>();

        asyncLock.Invoking(l => l.LockSync())
                 .Should()
                 .Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task Dispose_FailsQueuedWaiters()
    {
        using var asyncLock = new AsyncLock();
        var firstAcquired = false;

        // First task holds the lock
        Task firstTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            firstAcquired = true;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);
        firstAcquired.Should().BeTrue();

        // Second task waits
        Task<Releaser> secondTask = asyncLock.Lock(TestContext.Current.CancellationToken)
                                             .AsTask();

        await Task.Delay(10, TestContext.Current.CancellationToken);
        asyncLock.Dispose();

        Func<Task> act = async () => await secondTask;
        await act.Should().ThrowAsync<ObjectDisposedException>();
        await firstTask;
    }

    [Fact]
    public async Task Dispose_AllowsCurrentHolderToComplete()
    {
        var asyncLock = new AsyncLock();
        var completed = false;

        Task task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            await Task.Delay(50, TestContext.Current.CancellationToken);
            completed = true;
        }, TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);
        asyncLock.Dispose();
        await task;

        completed.Should().BeTrue();
    }

    [Fact]
    public async Task DisposeAsync_WaitsForCurrentHolder()
    {
        var asyncLock = new AsyncLock();
        var released = false;

        var acquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRelease = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            acquired.TrySetResult(true);

            await allowRelease.Task;
            released = true;
        }, TestContext.Current.CancellationToken);

        await acquired.Task.WaitAsync(TestContext.Current.CancellationToken);

        ValueTask disposeTask = asyncLock.DisposeAsync();

        // DisposeAsync should wait for the current holder to exit.
        disposeTask.IsCompleted.Should().BeFalse();

        allowRelease.TrySetResult(true);

        await task;
        await disposeTask;

        released.Should().BeTrue();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Invoking(l => l.Dispose()).Should().NotThrow();
        asyncLock.Invoking(l => l.Dispose()).Should().NotThrow(); // Should not throw
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        var act1 = asyncLock.Awaiting(l => l.DisposeAsync().AsTask());
        await act1.Should().NotThrowAsync();

        var act2 = asyncLock.Awaiting(l => l.DisposeAsync().AsTask());
        await act2.Should().NotThrowAsync(); // Should not throw
    }

    [Fact]
    public async Task LockAsync_MixedWithLockSync_WorksCorrectly()
    {
        using var asyncLock = new AsyncLock();
        var order = new System.Collections.Concurrent.ConcurrentQueue<string>();

        // Async first
        Task asyncTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            order.Enqueue("async1");
            await Task.Delay(30, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);

        await Task.Delay(5, TestContext.Current.CancellationToken);

        // Sync waits
        var syncThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync(TestContext.Current.CancellationToken);
            order.Enqueue("sync");
        });

        syncThread.Start();
        await Task.Delay(5, TestContext.Current.CancellationToken);

        // Another async waits
        Task asyncTask2 = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            order.Enqueue("async2");
        }, TestContext.Current.CancellationToken);

        await asyncTask;
        syncThread.Join();
        await asyncTask2;

        string[] results = order.ToArray();
        results.Should().Equal("async1", "sync", "async2");
    }

    [Fact]
    public async Task Releaser_Dispose_ReleasesLock()
    {
        using var asyncLock = new AsyncLock();
        var secondAcquired = false;

        Task firstTask = Task.Run(async () =>
        {
            Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            await Task.Delay(30, TestContext.Current.CancellationToken);
            releaser.Dispose(); // Explicit dispose
        }, TestContext.Current.CancellationToken);

        await Task.Delay(10, TestContext.Current.CancellationToken);

        Task secondTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
            secondAcquired = true;
        }, TestContext.Current.CancellationToken);

        await firstTask;
        await Task.Delay(10, TestContext.Current.CancellationToken);
        await secondTask;

        secondAcquired.Should().BeTrue();
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
                    using Releaser releaser = await asyncLock.Lock(TestContext.Current.CancellationToken);
                    Interlocked.Increment(ref count);
                    await Task.Yield();
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(tasks);

        count.Should().Be(100);
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
                    using Releaser releaser = asyncLock.LockSync(TestContext.Current.CancellationToken);
                    Interlocked.Increment(ref count);
                }
            });
            threads[i].Start();
        }

        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        count.Should().Be(100);
    }

    [Fact]
    public async Task LockAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        var act = asyncLock.Awaiting(l => l.Lock().AsTask());
        var ex = await act.Should().ThrowAsync<ObjectDisposedException>();

        ex.And.ObjectName.Should().Be(nameof(AsyncLock));
    }

    [Fact]
    public void LockSync_AfterDispose_ThrowsObjectDisposedException()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        var ex = asyncLock.Invoking(l => l.LockSync())
                          .Should()
                          .Throw<ObjectDisposedException>();

        ex.And.ObjectName.Should().Be(nameof(AsyncLock));
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
                    await asyncLock.Lock(TestContext.Current.CancellationToken);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.Delay(10, TestContext.Current.CancellationToken);
        asyncLock.Dispose();

        await Task.WhenAll(acquireTasks);

        // All should have either succeeded before dispose or gotten ObjectDisposedException
        (exceptions.Count == 0 || exceptions.All(e => e is ObjectDisposedException)).Should().BeTrue();
    }
}