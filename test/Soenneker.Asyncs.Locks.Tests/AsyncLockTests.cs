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
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    // Keep timeouts generous to avoid CI noise, but always bounded to prevent hangs.
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    private static CancellationToken TimeoutToken(TimeSpan? timeout = null)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        cts.CancelAfter(timeout ?? DefaultTimeout);
        return cts.Token;
    }

    private static TaskCompletionSource<bool> NewTcs() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task LockAsync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = await asyncLock.Lock(TestToken);
        releaser.Dispose();
    }

    [Fact]
    public void LockSync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        Releaser releaser = asyncLock.LockSync(TestToken);
        releaser.Dispose();
    }

    [Fact]
    public async Task LockAsync_Contended_WaitsForRelease()
    {
        using var asyncLock = new AsyncLock();
        // Hold the lock on the test thread to avoid scheduling races.
        using Releaser first = await asyncLock.Lock(TestToken);

        Task<Releaser> secondTask = asyncLock.Lock(TestToken).AsTask();
        secondTask.IsCompleted.Should().BeFalse();

        first.Dispose();

        using Releaser second = await secondTask.WaitAsync(TimeoutToken());
        second.Dispose();
    }

    [Fact]
    public void LockSync_Contended_WaitsForRelease()
    {
        using var asyncLock = new AsyncLock();
        // Hold the lock on the test thread to avoid ordering/scheduling races.
        using Releaser first = asyncLock.LockSync(TestToken);

        var secondAcquired = new ManualResetEventSlim(false);

        var secondThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync(TestToken);
            secondAcquired.Set();
        });

        secondThread.Start();

        // While we still hold the lock, the second thread cannot have acquired it.
        secondAcquired.IsSet.Should().BeFalse();

        first.Dispose();

        secondAcquired.Wait(DefaultTimeout).Should().BeTrue();
        secondThread.Join();
    }

    [Fact]
    public async Task LockAsync_MultipleWaiters_ProcessesInOrder()
    {
        using var asyncLock = new AsyncLock();
        var order = new System.Collections.Concurrent.ConcurrentQueue<int>();

        // Hold the lock so waiter creation/queuing is deterministic (no Task scheduling).
        using Releaser first = await asyncLock.Lock(TestToken);
        order.Enqueue(1);

        Task<Releaser> secondTask = asyncLock.Lock(TestToken).AsTask();
        Task<Releaser> thirdTask = asyncLock.Lock(TestToken).AsTask();

        first.Dispose();

        using (Releaser second = await secondTask.WaitAsync(TimeoutToken()))
        {
            order.Enqueue(2);
        }

        using (Releaser third = await thirdTask.WaitAsync(TimeoutToken()))
        {
            order.Enqueue(3);
        }

        int[] results = order.ToArray();
        results.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task LockAsync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // Hold the lock on the test thread so the second acquisition is guaranteed to wait.
        using Releaser first = await asyncLock.Lock(TestToken);

        // Second task waits with cancellation
        Task<Releaser> secondTask = asyncLock.Lock(cts.Token)
                                             .AsTask();

        secondTask.IsCompleted.Should().BeFalse();
        cts.Cancel();

        Func<Task> act = async () => await secondTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
        first.Dispose();
    }

    [Fact]
    public async Task LockAsync_CancelVsRelease_Race_ObservesOneOutcome()
    {
        using var asyncLock = new AsyncLock();

        for (int i = 0; i < 200; i++)
        {
            Releaser holder = await asyncLock.Lock(TestToken);
            using var cts = new CancellationTokenSource();

            Task<Releaser> waiterTask = asyncLock.Lock(cts.Token).AsTask();
            waiterTask.IsCompleted.Should().BeFalse();

            var start = new ManualResetEventSlim(false);

            Task cancelTask = Task.Run(() =>
            {
                start.Wait();
                cts.Cancel();
            }, TestToken);

            Task releaseTask = Task.Run(() =>
            {
                start.Wait();
                holder.Dispose();
            }, TestToken);

            start.Set();

            Task completed = await Task.WhenAny(waiterTask, Task.Delay(DefaultTimeout, TestToken));
            completed.Should().Be(waiterTask);

            bool acquired = false;
            Releaser next = default;

            try
            {
                next = await waiterTask;
                acquired = true;
            }
            catch (OperationCanceledException ex)
            {
                ex.CancellationToken.Should().Be(cts.Token);
            }

            await Task.WhenAll(cancelTask, releaseTask).WaitAsync(TimeoutToken());

            if (acquired)
                next.Dispose();
        }
    }

    [Fact]
    public void LockSync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // Hold the lock on the test thread.
        using Releaser first = asyncLock.LockSync();

        // Second thread waits with cancellation
        var canceled = false;
        var secondFinished = new ManualResetEventSlim(false);
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
            finally
            {
                secondFinished.Set();
            }
        });

        secondThread.Start();
        cts.Cancel();
        secondFinished.Wait(DefaultTimeout).Should().BeTrue();
        secondThread.Join();

        canceled.Should().BeTrue();
        first.Dispose();
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
        var allowRelease = NewTcs();
        var acquired = NewTcs();

        // Holder task acquires, then waits.
        Task holderTask = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestToken);
            acquired.TrySetResult(true);
            await allowRelease.Task.WaitAsync(TimeoutToken());
        }, TestToken);

        await acquired.Task.WaitAsync(TimeoutToken());

        // Second task waits
        Task<Releaser> secondTask = asyncLock.Lock(TestToken)
                                             .AsTask();

        asyncLock.Dispose();

        Func<Task> act = async () => await secondTask;
        await act.Should().ThrowAsync<ObjectDisposedException>();

        allowRelease.TrySetResult(true);
        await holderTask.WaitAsync(TimeoutToken());
    }

    [Fact]
    public async Task Dispose_ConcurrentRelease_CompletesWithValidOutcome()
    {
        for (int i = 0; i < 200; i++)
        {
            var asyncLock = new AsyncLock();

            Releaser holder = await asyncLock.Lock(TestToken);
            Task<Releaser> waiterTask = asyncLock.Lock(TestToken).AsTask();
            waiterTask.IsCompleted.Should().BeFalse();

            var start = new ManualResetEventSlim(false);

            Task disposeTask = Task.Run(() =>
            {
                start.Wait();
                asyncLock.Dispose();
            }, TestToken);

            Task releaseTask = Task.Run(() =>
            {
                start.Wait();
                holder.Dispose();
            }, TestToken);

            start.Set();
            await Task.WhenAll(disposeTask, releaseTask).WaitAsync(TimeoutToken());

            if (waiterTask.IsCompletedSuccessfully)
            {
                using Releaser acquired = await waiterTask;
            }
            else
            {
                Func<Task> act = async () => await waiterTask;
                await act.Should().ThrowAsync<ObjectDisposedException>();
            }
        }
    }

    [Fact]
    public async Task Dispose_AllowsCurrentHolderToComplete()
    {
        var asyncLock = new AsyncLock();
        var allowExit = NewTcs();
        var acquired = NewTcs();
        var completed = new ManualResetEventSlim(false);

        Task holder = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestToken);
            acquired.TrySetResult(true);
            await allowExit.Task.WaitAsync(TimeoutToken());
            completed.Set();
        }, TestToken);

        await acquired.Task.WaitAsync(TimeoutToken());
        asyncLock.Dispose();
        allowExit.TrySetResult(true);
        await holder.WaitAsync(TimeoutToken());

        completed.IsSet.Should().BeTrue();
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

        // Hold lock (async) first.
        using Releaser first = await asyncLock.Lock(TestToken);
        order.Enqueue("async1");

        // Queue both sync + async contenders while lock is held.
        // IMPORTANT: whichever contender acquires first must release promptly to avoid deadlocks.
        var syncFinished = new ManualResetEventSlim(false);
        var syncThread = new Thread(() =>
        {
            using Releaser releaser = asyncLock.LockSync(TestToken);
            order.Enqueue("sync");
            syncFinished.Set();
        });
        syncThread.Start();

        Task async2Task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestToken);
            order.Enqueue("async2");
        }, TestToken);

        // Release and wait for both contenders to complete.
        first.Dispose();

        syncFinished.Wait(DefaultTimeout).Should().BeTrue();
        syncThread.Join();
        await async2Task.WaitAsync(TimeoutToken());

        string[] results = order.ToArray();
        // Deterministic requirement: async1 must happen first, and both contenders must run after.
        results.Length.Should().Be(3);
        results[0].Should().Be("async1");
        results.Skip(1).Should().BeEquivalentTo(new[] { "sync", "async2" });
    }

    [Fact]
    public async Task Releaser_Dispose_ReleasesLock()
    {
        using var asyncLock = new AsyncLock();
        using Releaser first = await asyncLock.Lock(TestToken);

        Task<Releaser> secondTask = asyncLock.Lock(TestToken).AsTask();
        secondTask.IsCompleted.Should().BeFalse();

        first.Dispose(); // Explicit dispose

        using Releaser second = await secondTask.WaitAsync(TimeoutToken());
        second.Dispose();
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
                    using Releaser releaser = await asyncLock.Lock(TestToken);
                    Interlocked.Increment(ref count);
                    await Task.Yield();
                }
            }, TestToken);
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
                    using Releaser releaser = asyncLock.LockSync(TestToken);
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

        // Multiple tasks trying to acquire (start together, then dispose concurrently).
        var start = NewTcs();
        var acquireTasks = new Task[5];
        for (int i = 0; i < 5; i++)
        {
            acquireTasks[i] = Task.Run(async () =>
            {
                await start.Task.WaitAsync(TimeoutToken());
                try
                {
                    await asyncLock.Lock(TestToken);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }, TestToken);
        }

        start.TrySetResult(true);
        asyncLock.Dispose();

        await Task.WhenAll(acquireTasks).WaitAsync(TimeoutToken());

        // All should have either succeeded before dispose or gotten ObjectDisposedException
        (exceptions.Count == 0 || exceptions.All(e => e is ObjectDisposedException)).Should().BeTrue();
    }
}