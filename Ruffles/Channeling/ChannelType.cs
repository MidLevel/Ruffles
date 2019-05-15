namespace Ruffles.Channeling
{
    public enum ChannelType : byte
    {
        Reliable,
        Unreliable,
        UnreliableSequenced,
        ReliableSequenced,
        UnreliableRaw
    }
}
