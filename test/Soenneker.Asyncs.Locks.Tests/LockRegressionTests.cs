using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Locks.Tests;

public sealed class LockRegressionTests
{
    [Test]
    [Arguments(2)]
    [Arguments(8)]
    public async ValueTask Direct_and_overflow_handoffs_preserve_exclusion(int workers, CancellationToken cancellationToken)
    {
        using var gate = new AsyncLock();
        int holders = 0, violations = 0, completed = 0;
        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 10000; i++)
            {
                using Releaser lease = await gate.Lock(cancellationToken: cancellationToken);
                if (Interlocked.Increment(ref holders) != 1)
                    Interlocked.Increment(ref violations);
                if ((i & 7) == 0)
                    await Task.Yield();
                completed++;
                Interlocked.Decrement(ref holders);
            }
        }, cancellationToken: cancellationToken))).WaitAsync(TimeSpan.FromSeconds(15), cancellationToken: cancellationToken);
        await Assert.That(violations).IsEqualTo(0);
        await Assert.That(completed).IsEqualTo(workers * 10000);
    }
}
