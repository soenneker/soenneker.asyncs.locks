using System.Threading.Tasks;
using BenchmarkDotNet.Reports;
using Soenneker.Benchmarking.Extensions.Summary;
using Soenneker.Facts.Local;
using Soenneker.Facts.Manual;
using Soenneker.Tests.Benchmark;
using Xunit;

namespace Soenneker.Asyncs.Locks.Tests.Benchmarks;

public class BenchmarkRunner : BenchmarkTest
{
    public BenchmarkRunner(ITestOutputHelper outputHelper) : base(outputHelper)
    {
    }

    //[ManualFact]
    // [LocalFact]
    public async ValueTask Lock()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<LockBenchmark>(DefaultConf);

        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    //[ManualFact]
    // [LocalFact]
    public async ValueTask LockSingleWaiterHandoff()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<SingleWaiterHandoffBenchmark>(DefaultConf);
        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    //[ManualFact]
    // [LocalFact]
    public async ValueTask LockThroughputContention()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<ThroughputContentionBenchmark>(DefaultConf);
        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    //[ManualFact]
    //   [LocalFact]
    public async ValueTask LockHoldTimeContention()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<HoldTimeContentionBenchmark>(DefaultConf);
        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    //[ManualFact]
    //  [LocalFact]
    public async ValueTask LockOverWork()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<LockOverWorkBenchmark>(DefaultConf);

        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    // [ManualFact]
    //  [LocalFact]
    public async ValueTask LockSync()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<LockSyncBenchmark>(DefaultConf);

        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }

    [ManualFact]
    // [LocalFact]
    public async ValueTask LockSyncOverWork()
    {
        Summary summary = BenchmarkDotNet.Running.BenchmarkRunner.Run<LockSyncOverWorkBenchmark>(DefaultConf);

        await summary.OutputSummaryToLog(OutputHelper, CancellationToken);
    }
}