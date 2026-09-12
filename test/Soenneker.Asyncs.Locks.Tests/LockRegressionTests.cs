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
    public async Task Direct_and_overflow_handoffs_preserve_exclusion(int workers)
    {
        using var gate = new AsyncLock();
        int holders = 0, violations = 0, completed = 0;
        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 10000; i++)
            {
                using Releaser lease = await gate.Lock();
                if (Interlocked.Increment(ref holders) != 1)
                    Interlocked.Increment(ref violations);
                if ((i & 7) == 0)
                    await Task.Yield();
                completed++;
                Interlocked.Decrement(ref holders);
            }
        }))).WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.That(violations).IsEqualTo(0);
        await Assert.That(completed).IsEqualTo(workers * 10000);
    }
}
