using Soenneker.Queues.Intrusive.ValueMpsc;

namespace Soenneker.Asyncs.Locks;

// Allocate padded queue storage only when more than the direct waiter slot is needed.
internal sealed class WaiterQueue
{
    internal ValueIntrusiveMpscReclaimingQueue<Waiter> Queue;

    internal WaiterQueue() => Queue = new ValueIntrusiveMpscReclaimingQueue<Waiter>(Waiter.Rent());
}
