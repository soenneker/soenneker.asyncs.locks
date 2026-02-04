namespace Soenneker.Asyncs.Locks.Tests.Enums
{
    public enum HoldMode
    {
        None = 0,
        SpinWait = 1,
        Yield = 2,
        Delay = 3
    }
}
