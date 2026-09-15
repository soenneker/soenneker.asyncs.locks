using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks.Tests;

public sealed class SynchronousWaiterTests
{
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task Blocked_waiter_wakes_for_grant_cancellation_disposal_and_interruption(int outcome)
    {
        using var gate = new AsyncLock();
        using var cancellation = new CancellationTokenSource();
        Releaser held = gate.LockSync();
        bool released = false;
        var completion = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = new Thread(() =>
        {
            try
            {
                using Releaser acquired = gate.LockSync(cancellation.Token);
                completion.SetResult(null);
            }
            catch (Exception exception)
            {
                completion.SetResult(exception);
            }
        }) { IsBackground = true };

        worker.Start();
        try
        {
            if (!SpinWait.SpinUntil(() => (worker.ThreadState & ThreadState.WaitSleepJoin) != 0, TimeSpan.FromSeconds(10)))
                throw new TimeoutException("The synchronous waiter did not block.");

            switch (outcome)
            {
                case 0: held.Dispose(); released = true; break;
                case 1: cancellation.Cancel(); break;
                case 2: gate.Dispose(); break;
                case 3: worker.Interrupt(); break;
            }

            Exception? error = await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(error?.GetType()).IsEqualTo(outcome switch
            {
                1 => typeof(OperationCanceledException),
                2 => typeof(ObjectDisposedException),
                3 => typeof(ThreadInterruptedException),
                _ => null
            });
            if (error is OperationCanceledException canceled)
                await Assert.That(canceled.CancellationToken).IsEqualTo(cancellation.Token);
        }
        finally
        {
            if (!released)
                held.Dispose();
            gate.Dispose();
            worker.Join(TimeSpan.FromSeconds(10));
        }

        await Assert.That(worker.IsAlive).IsFalse();
    }

    [Test]
    public async Task Cancellation_racing_a_synchronous_grant_does_not_leak_ownership()
    {
        using var gate = new AsyncLock();
        for (int i = 0; i < 256; i++)
        {
            using var cancellation = new CancellationTokenSource();
            Releaser held = gate.LockSync();
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var worker = new Thread(() =>
            {
                try
                {
                    using Releaser acquired = gate.LockSync(cancellation.Token);
                    completion.SetResult();
                }
                catch (OperationCanceledException)
                {
                    completion.SetResult();
                }
                catch (Exception exception)
                {
                    completion.SetException(exception);
                }
            }) { IsBackground = true };
            worker.Start();
            if (!SpinWait.SpinUntil(() => (worker.ThreadState & ThreadState.WaitSleepJoin) != 0, TimeSpan.FromSeconds(10)))
            {
                held.Dispose();
                throw new TimeoutException("The synchronous waiter did not block.");
            }

            Parallel.Invoke(cancellation.Cancel, held.Dispose);
            await completion.Task.WaitAsync(TimeSpan.FromSeconds(10));
            if (!worker.Join(TimeSpan.FromSeconds(10)))
                throw new TimeoutException();
            using Releaser check = await gate.Lock().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Test]
    public async Task Mixed_sync_and_async_waiters_preserve_exclusion_and_reuse()
    {
        using var gate = new AsyncLock();
        int holders = 0, violations = 0, completed = 0;
        void Enter()
        {
            if (Interlocked.Increment(ref holders) != 1)
                Interlocked.Increment(ref violations);
            completed++;
        }
        Task[] synchronous = Enumerable.Range(0, 4).Select(_ => Task.Factory.StartNew(() =>
        {
            for (int i = 0; i < 2000; i++)
            {
                using Releaser lease = gate.LockSync();
                Enter();
                Thread.Yield();
                Interlocked.Decrement(ref holders);
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
        Task[] asynchronous = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 2000; i++)
            {
                using Releaser lease = await gate.Lock();
                Enter();
                await Task.Yield();
                Interlocked.Decrement(ref holders);
            }
        })).ToArray();
        await Task.WhenAll(synchronous.Concat(asynchronous)).WaitAsync(TimeSpan.FromSeconds(20));
        await Assert.That(violations).IsEqualTo(0);
        await Assert.That(completed).IsEqualTo(16000);
    }
}
