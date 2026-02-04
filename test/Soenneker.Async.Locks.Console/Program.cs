using System;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Asyncs.Locks;

namespace Soenneker.Async.Locks.Console;

internal static class Program
{
    private static async Task Main()
    {
        // ---- knobs ----
        TimeSpan duration = TimeSpan.FromSeconds(5);
        var workers = 10;                 // try: Environment.ProcessorCount, 2x, 4x
        var workInside = 0;               // 0..200 (spin work in crit section)
        var cancelRate = 0.0;             // 0.0 .. 0.2
        var useToken = false;             // test with/without token registration

        var l = new AsyncLock();

        // Warmup JIT
        await Run(l, workers, TimeSpan.FromSeconds(2), workInside, cancelRate, useToken);

        System.Console.WriteLine("Running...");
        long ops = await Run(l, workers, duration, workInside, cancelRate, useToken);

        System.Console.WriteLine($"Workers: {workers}");
        System.Console.WriteLine($"Duration: {duration.TotalSeconds:n1}s");
        System.Console.WriteLine($"Ops: {ops:n0}");
        System.Console.WriteLine($"Ops/sec: {ops / duration.TotalSeconds:n0}");
    }

    private static async Task<long> Run(
        AsyncLock l,
        int workers,
        TimeSpan duration,
        int workInside,
        double cancelRate,
        bool useToken)
    {
        using var done = new CancellationTokenSource(duration);

        Task<long>[] tasks = new Task<long>[workers];

        for (int i = 0; i < workers; i++)
        {
            int workerId = i;

            tasks[i] = Task.Run(async () =>
            {
                // per-task local counter (no contention)
                long localOps = 0;

                // stable-ish seed; collisions aren't fatal for this benchmark
                var rnd = new Random(unchecked(Environment.TickCount ^ (workerId * 397)));

                while (!done.IsCancellationRequested)
                {
                    CancellationToken token = CancellationToken.None;
                    CancellationTokenSource? cts = null;

                    if (useToken)
                    {
                        if (cancelRate > 0 && rnd.NextDouble() < cancelRate)
                        {
                            cts = new CancellationTokenSource();
                            cts.Cancel();
                            token = cts.Token;
                        }
                        else
                        {
                            token = done.Token;
                        }
                    }

                    try
                    {
                        if (useToken)
                        {
                            using Releaser r = await l.Lock(token);
                            if (workInside != 0) Spin(workInside);
                        }
                        else
                        {
                            using Releaser r = await l.Lock();
                            if (workInside != 0) Spin(workInside);
                        }

                        localOps++;
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                    finally
                    {
                        cts?.Dispose();
                    }
                }

                return localOps;
            });
        }

        long[] results = await Task.WhenAll(tasks);

        long total = 0;
        for (int i = 0; i < results.Length; i++)
            total += results[i];

        return total;
    }

    private static void Spin(int iterations)
    {
        Thread.SpinWait(iterations);
    }
}
