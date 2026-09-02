using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using AwesomeAssertions.Specialized;

namespace Soenneker.Asyncs.Locks.Tests;

public sealed class AsyncLockTests
{
    private static CancellationToken TestToken => CancellationToken.None;

    // Keep timeouts generous to avoid CI noise, but always bounded to prevent hangs.
    private static readonly TimeSpan _defaultTimeout = TimeSpan.FromSeconds(10);

    private static CancellationToken TimeoutToken(TimeSpan? timeout = null)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        cts.CancelAfter(timeout ?? _defaultTimeout);
        return cts.Token;
    }

    private static TaskCompletionSource<bool> NewTcs() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Test]
    public async Task LockAsync_Uncontended_AcquiresImmediately()
    {
        await using var asyncLock = new AsyncLock();
        using Releaser releaser = await asyncLock.Lock(TestToken);
    }

    [Test]
    public void LockSync_Uncontended_AcquiresImmediately()
    {
        using var asyncLock = new AsyncLock();
        using Releaser releaser = asyncLock.LockSync(TestToken);
    }

    [Test]
    public async Task LockAsync_Contended_WaitsForRelease()
    {
        await using var asyncLock = new AsyncLock();
        // Hold the lock on the test thread to avoid scheduling races.
        Releaser first = await asyncLock.Lock(TestToken);

        Task<Releaser> secondTask = asyncLock.Lock(TestToken).AsTask();
        secondTask.IsCompleted.Should().BeFalse();

        first.Dispose();

        using Releaser second = await secondTask.WaitAsync(TimeoutToken());
    }

    [Test]
    public void LockSync_Contended_WaitsForRelease()
    {
        using var asyncLock = new AsyncLock();
        // Hold the lock on the test thread to avoid ordering/scheduling races.
        Releaser first = asyncLock.LockSync(TestToken);

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

        secondAcquired.Wait(_defaultTimeout, TestToken).Should().BeTrue();
        secondThread.Join();
    }

    [Test]
    public async Task LockAsync_MultipleWaiters_ProcessesInOrder()
    {
        const int waiters = 10;
        await using var asyncLock = new AsyncLock();
        
        var lockTasks = new List<Task>();
        var order = new ConcurrentQueue<int>();
        var allowRelease = new ManualResetEventSlim(false);

        for (int i = 0; i < waiters; i++)
        {
            Task<Releaser> task = asyncLock.Lock(TestToken).AsTask();
            int taskIndex = i;
            lockTasks.Add(task.ContinueWith(LockAcquired));
            continue;

            async Task LockAcquired(Task<Releaser> acquiredTask)
            {
                using Releaser _ = await acquiredTask;
                order.Enqueue(taskIndex);
                allowRelease.Wait(TimeoutToken());
            }
        }
        
        allowRelease.Set();
        await Task.WhenAll(lockTasks);
        
        int[] results = [..order];
        results.Should().Equal([..Enumerable.Range(0, waiters)]);
    }

    [Test]
    public async Task LockAsync_WithCancellation_CancelsWhenRequested()
    {
        await using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // Hold the lock on the test thread so the second acquisition is guaranteed to wait.
        using Releaser first = await asyncLock.Lock(TestToken);

        // Second task waits with cancellation
        Task<Releaser> secondTask = asyncLock.Lock(cts.Token)
                                             .AsTask();

        secondTask.IsCompleted.Should().BeFalse();
        await cts.CancelAsync();

        Func<Task> act = async () => await secondTask;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Test]
    public async Task LockAsync_CancelVsRelease_Race_ObservesOneOutcome()
    {
        await using var asyncLock = new AsyncLock();

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

            Task completed = await Task.WhenAny(waiterTask, Task.Delay(_defaultTimeout, TestToken));
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

    [Test]
    public async Task LockAsync_CancelVsRelease_Race_Stress_ObservesOneOutcome()
    {
        await using var asyncLock = new AsyncLock();

        for (int i = 0; i < 5000; i++)
        {
            Releaser holder = await asyncLock.Lock(TestToken);
            using var cts = new CancellationTokenSource();

            Task<Releaser> waiterTask = asyncLock.Lock(cts.Token).AsTask();

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

            Task completed = await Task.WhenAny(waiterTask, Task.Delay(_defaultTimeout, TestToken));
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

    [Test]
    public void LockSync_WithCancellation_CancelsWhenRequested()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();

        // Hold the lock on the test thread.
        using Releaser first = asyncLock.LockSync(TestToken);

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
        secondFinished.Wait(_defaultTimeout, TestToken).Should().BeTrue();
        secondThread.Join();

        canceled.Should().BeTrue();
    }

    [Test]
    public async Task LockAsync_AlreadyCanceled_ThrowsImmediately()
    {
        await using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Func<Task<Releaser>>? act = asyncLock.Awaiting(l => l.Lock(cts.Token).AsTask());
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Test]
    public void LockSync_AlreadyCanceled_ThrowsImmediately()
    {
        using var asyncLock = new AsyncLock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        asyncLock.Invoking(l => l.LockSync(cts.Token))
                 .Should()
                 .Throw<OperationCanceledException>();
    }

    [Test]
    public async Task Dispose_PreventsNewAcquisitions(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        await asyncLock.DisposeAsync();

        Func<Task<Releaser>>? act = asyncLock.Awaiting(l => l.Lock(cancellationToken).AsTask());
        await act.Should().ThrowAsync<ObjectDisposedException>();

        asyncLock.Invoking(l => l.LockSync(cancellationToken))
                 .Should()
                 .Throw<ObjectDisposedException>();
    }

    [Test]
    public async Task Dispose_FailsQueuedWaiters()
    {
        await using var asyncLock = new AsyncLock();
        TaskCompletionSource<bool> allowRelease = NewTcs();
        TaskCompletionSource<bool> acquired = NewTcs();

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

    [Test]
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

            try
            {
                using Releaser acquired = await waiterTask;
            }
            catch (ObjectDisposedException)
            {
                // ignored
            }
        }
    }

    [Test]
    public async Task Dispose_ConcurrentRelease_Stress_CompletesWithValidOutcome()
    {
        for (int i = 0; i < 5000; i++)
        {
            var asyncLock = new AsyncLock();

            Releaser holder = await asyncLock.Lock(TestToken);
            Task<Releaser> waiterTask = asyncLock.Lock(TestToken).AsTask();

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

            try
            {
                using Releaser acquired = await waiterTask;
            }
            catch (ObjectDisposedException)
            {
                // ignored
            }
        }
    }

    [Test]
    public async Task Dispose_AllowsCurrentHolderToComplete()
    {
        var asyncLock = new AsyncLock();
        TaskCompletionSource<bool> allowExit = NewTcs();
        TaskCompletionSource<bool> acquired = NewTcs();
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

    [Test]
    public async Task DisposeAsync_WaitsForCurrentHolder()
    {
        var asyncLock = new AsyncLock();
        var released = false;

        var acquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRelease = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task task = Task.Run(async () =>
        {
            using Releaser releaser = await asyncLock.Lock(TestToken);
            acquired.TrySetResult(true);

            await allowRelease.Task;
            released = true;
        }, TestToken);

        await acquired.Task.WaitAsync(TestToken);

        ValueTask disposeTask = asyncLock.DisposeAsync();

        // DisposeAsync should wait for the current holder to exit.
        disposeTask.IsCompleted.Should().BeFalse();

        allowRelease.TrySetResult(true);

        await task;
        await disposeTask;

        released.Should().BeTrue();
    }

    [Test]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Invoking(l => l.Dispose()).Should().NotThrow();
        asyncLock.Invoking(l => l.Dispose()).Should().NotThrow(); // Should not throw
    }

    [Test]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var asyncLock = new AsyncLock();
        Func<Task>? act1 = asyncLock.Awaiting(l => l.DisposeAsync().AsTask());
        await act1.Should().NotThrowAsync();

        Func<Task>? act2 = asyncLock.Awaiting(l => l.DisposeAsync().AsTask());
        await act2.Should().NotThrowAsync(); // Should not throw
    }

    [Test]
    public async Task LockAsync_MixedWithLockSync_WorksCorrectly()
    {
        await using var asyncLock = new AsyncLock();
        var order = new ConcurrentQueue<string>();

        // Hold lock (async) first.
        Releaser first = await asyncLock.Lock(TestToken);
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

        syncFinished.Wait(_defaultTimeout, TestToken).Should().BeTrue();
        syncThread.Join();
        await async2Task.WaitAsync(TimeoutToken());

        string[] results = order.ToArray();
        // Deterministic requirement: async1 must happen first, and both contenders must run after.
        results.Length.Should().Be(3);
        results[0].Should().Be("async1");
        results.Skip(1).Should().BeEquivalentTo(["sync", "async2"]);
    }

    [Test]
    public async Task Releaser_Dispose_ReleasesLock()
    {
        await using var asyncLock = new AsyncLock();
        Releaser first = await asyncLock.Lock(TestToken);

        Task<Releaser> secondTask = asyncLock.Lock(TestToken).AsTask();
        secondTask.IsCompleted.Should().BeFalse();

        first.Dispose(); // Explicit dispose

        using Releaser second = await secondTask.WaitAsync(TimeoutToken());
    }

    [Test]
    public async Task LockAsync_RapidAcquireRelease_WorksCorrectly()
    {
        await using var asyncLock = new AsyncLock();
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

    [Test]
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

    [Test]
    public async Task LockAsync_AfterDispose_ThrowsObjectDisposedException(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        await asyncLock.DisposeAsync();

        Func<Task<Releaser>>? act = asyncLock.Awaiting(l => l.Lock(cancellationToken).AsTask());
        ExceptionAssertions<ObjectDisposedException>? ex = await act.Should().ThrowAsync<ObjectDisposedException>();

        ex.And.ObjectName.Should().Be(nameof(AsyncLock));
    }

    [Test]
    public void LockSync_AfterDispose_ThrowsObjectDisposedException()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        ExceptionAssertions<ObjectDisposedException>? ex = asyncLock.Invoking(l => l.LockSync())
                                                                    .Should()
                                                                    .Throw<ObjectDisposedException>();

        ex.And.ObjectName.Should().Be(nameof(AsyncLock));
    }

    [Test]
    public async Task LockAsync_ConcurrentDispose_HandlesGracefully()
    {
        var asyncLock = new AsyncLock();
        var exceptions = new ConcurrentBag<Exception>();

        // Multiple tasks trying to acquire (start together, then dispose concurrently).
        TaskCompletionSource<bool> start = NewTcs();
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

    [Test]
    public async Task LockAsync_ConcurrentDispose_Stress_HandlesGracefully()
    {
        for (int i = 0; i < 500; i++)
        {
            var asyncLock = new AsyncLock();
            var exceptions = new ConcurrentBag<Exception>();

            TaskCompletionSource<bool> start = NewTcs();
            var acquireTasks = new Task[10];
            for (int j = 0; j < acquireTasks.Length; j++)
            {
                acquireTasks[j] = Task.Run(async () =>
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

            (exceptions.Count == 0 || exceptions.All(e => e is ObjectDisposedException)).Should().BeTrue();
        }
    }
}
