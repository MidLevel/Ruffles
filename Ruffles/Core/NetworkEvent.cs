using System;
using Ruffles.Connections;
using Ruffles.Exceptions;
using Ruffles.Memory;

namespace Ruffles.Core
{
    /// <summary>
    /// Struct representing a network event.
    /// </summary>
    public struct NetworkEvent
    {
        /// <summary>
        /// Gets the event type.
        /// </summary>
        /// <value>The event type.</value>
        public NetworkEventType Type { get; internal set; }
        /// <summary>
        /// Gets the RuffleSocket the event occured on.
        /// </summary>
        /// <value>The RuffleSocket the event occured on.</value>
        public RuffleSocket Socket { get; internal set; }
        /// <summary>
        /// Gets the connection of the event.
        /// </summary>
        /// <value>The connection of the event.</value>
        public Connection Connection { get; internal set; }
        /// <summary>
        /// Gets an array segment of the borrowed memory. Only avalible when type is Data.
        /// Once used, the Recycle method should be called on the event to prevent a memory leak.
        /// </summary>
        /// <value>The data segement.</value>
        public ArraySegment<byte> Data { get; internal set; }
        /// <summary>
        /// Gets the time the event was received on the socket.
        /// Useful for calculating exact receive times.
        /// </summary>
        /// <value>The socket receive time.</value>
        public DateTime SocketReceiveTime { get; internal set; }
        /// <summary>
        /// Gets the channelId the message was sent over.
        /// </summary>
        /// <value>The channelId the message was sent over.</value>
        public byte ChannelId { get; internal set; }

        internal HeapMemory InternalMemory;
        internal bool AllowUserRecycle;

        /// <summary>
        /// Recycles the memory associated with the event.
        /// </summary>
        public void Recycle()
        {
            if (InternalMemory != null)
            {
                if (!AllowUserRecycle)
                {
                    throw new MemoryException("Cannot deallocate non recyclable memory");
                }

                MemoryManager.DeAlloc(InternalMemory);
            }
        }
    }
}
