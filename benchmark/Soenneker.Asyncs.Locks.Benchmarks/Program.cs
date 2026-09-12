using System;
using System.Reflection;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Soenneker.Asyncs.Locks.Tests.Benchmarks;
using Soenneker.Asyncs.Locks.Tests.Enums;

if (args.Length == 1 && args[0] == "--smoke")
{
    Type[] types =
    [
        typeof(LockBenchmark), typeof(LockSyncBenchmark),
        typeof(LockOverWorkBenchmark), typeof(LockSyncOverWorkBenchmark),
        typeof(SingleWaiterHandoffBenchmark), typeof(ThroughputContentionBenchmark),
        typeof(HoldTimeContentionBenchmark)
    ];

    foreach (Type type in types)
    {
        foreach (MethodInfo method in type.GetMethods())
        {
            if (method.GetCustomAttribute<BenchmarkAttribute>() is null)
                continue;

            HoldMode[] holds = type == typeof(HoldTimeContentionBenchmark)
                ? [HoldMode.SpinWait, HoldMode.Yield, HoldMode.Delay]
                : [HoldMode.None];

            foreach (HoldMode hold in holds)
            {
                object instance = Activator.CreateInstance(type)!;
                foreach (FieldInfo field in type.GetFields())
                {
                    ParamsAttribute? parameters = field.GetCustomAttribute<ParamsAttribute>();
                    if (parameters is not null)
                        field.SetValue(instance, parameters.Values[0]);
                }

                type.GetField("Contenders")?.SetValue(instance, 4);
                type.GetField("OpsPerWorker")?.SetValue(instance, 16);
                type.GetField("Hold")?.SetValue(instance, hold);
                type.GetMethod("Setup")!.Invoke(instance, null);
                try
                {
                    // Repeat on the same instance to catch stale gates/counters.
                    for (int repeat = 0; repeat < 2; repeat++)
                    {
                        object? result = method.Invoke(instance, null);
                        if (result is Task task)
                            await task.WaitAsync(TimeSpan.FromSeconds(30));
                        else if (result is ValueTask valueTask)
                            await valueTask.AsTask().WaitAsync(TimeSpan.FromSeconds(30));
                    }
                    Console.WriteLine($"PASS {type.Name}.{method.Name} ({hold})");
                }
                finally
                {
                    type.GetMethod("Cleanup")!.Invoke(instance, null);
                }
            }
        }
    }
}
else
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
