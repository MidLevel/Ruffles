namespace Ruffles.Channeling
{
    /// <summary>
    /// Enum representing different delivery methods.
    /// </summary>
    public enum ChannelType : byte
    {
        /// <summary>
        /// All messages are guaranteed to be delivered, the order is not guaranteed. 
        /// Duplicate packets are dropped.
        /// </summary>
        Reliable,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is not guaranteed.
        /// Duplicate packets are dropped.
        /// </summary>
        Unreliable,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is guaranteed.
        /// Older packets and duplicate packets are dropped.
        /// </summary>
        UnreliableSequenced,
        /// <summary>
        /// All messages are guaranteed to be delivered, the order is guaranteed. 
        /// Duplicate packets are dropped.
        /// </summary>
        ReliableSequenced,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is not guaranteed.
        /// Duplicate packets are not dropped.
        /// </summary>
        UnreliableRaw,
        /// <summary>
        /// Messages are guaranteed to be delivered, the order is guaranteed.
        /// Messages can be of a size larger than the MTU.
        /// Duplicate packets are dropped
        /// </summary>
        ReliableSequencedFragmented,
        /// <summary>
        /// All messages are not guaranteed to be delivered, the order is guaranteed.
        /// If sending multiple messages, at least one message is guaranteed to be delivered.
        /// Duplicate packets are dropped
        /// </summary>
        ReliableStateUpdate
    }
}
