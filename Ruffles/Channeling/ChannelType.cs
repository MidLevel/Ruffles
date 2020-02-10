using System;

namespace Ruffles.Channeling
{
    /// <summary>
    /// Enum representing different delivery methods.
    /// </summary>
    [Flags]
    public enum ChannelType : int
    {
        /// <summary>
        /// All messages are guaranteed to be delivered, the order is not guaranteed. 
        /// Duplicate packets are dropped.
        /// </summary>
        Reliable = 1,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is not guaranteed.
        /// Duplicate packets are dropped.
        /// </summary>
        Unreliable = 2,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is guaranteed.
        /// Older packets and duplicate packets are dropped.
        /// </summary>
        UnreliableOrdered = 4,
        /// <summary>
        /// All messages are guaranteed to be delivered, the order is guaranteed. 
        /// Duplicate packets are dropped.
        /// </summary>
        ReliableSequenced = 8,
        /// <summary>
        /// Messages are not guaranteed to be delivered, the order is not guaranteed.
        /// Duplicate packets are not dropped.
        /// </summary>
        UnreliableRaw = 16,
        /// <summary>
        /// Messages are guaranteed to be delivered, the order is guaranteed.
        /// Messages can be of a size larger than the MTU.
        /// Duplicate packets are dropped
        /// </summary>
        ReliableSequencedFragmented = 32,
        /// <summary>
        /// All messages are not guaranteed to be delivered, the order is guaranteed.
        /// If sending multiple messages, at least one message is guaranteed to be delivered.
        /// Duplicate packets are dropped
        /// </summary>
        ReliableOrdered = 64,
        /// <summary>
        /// All messages are guaranteed to be delivered, the order is not guaranteed.
        /// Messages can be of a size larger than the MTU.
        /// Duplicate packets are dropped.
        /// </summary>
        ReliableFragmented = 128
    }
}
