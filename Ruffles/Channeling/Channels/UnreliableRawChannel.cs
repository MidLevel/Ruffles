using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;

namespace Ruffles.Channeling.Channels
{
    internal class UnreliableRawChannel : IChannel
    {
        // Channel info
        private readonly byte channelId;
        private readonly Connection connection;
        private readonly SocketConfig config;

        internal UnreliableRawChannel(byte channelId, Connection connection, SocketConfig config)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
        }

        public HeapMemory CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            // Allocate the memory
            HeapMemory memory = MemoryManager.Alloc((uint)payload.Count + 2);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
            memory.Buffer[1] = channelId;

            // Copy the payload
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 2, payload.Count);

            // Tell the caller to deallc the memory
            dealloc = true;

            return memory;
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Unreliable messages have no acks.
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out bool hasMore)
        {
            // Unreliable has one message in equal no more than one out.
            hasMore = false;

            return new ArraySegment<byte>(payload.Array, payload.Offset, payload.Count);
        }

        public HeapMemory HandlePoll()
        {
            return null;
        }

        public void InternalUpdate()
        {
            // Unreliable doesnt need to resend, thus no internal loop is required
        }

        public void Reset()
        {
            // UnreliableRaw has nothing to clean up
        }
    }
}
