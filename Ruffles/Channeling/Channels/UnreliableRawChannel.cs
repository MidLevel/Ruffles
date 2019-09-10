using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class UnreliableRawChannel : IChannel
    {
        // Channel info
        private readonly byte channelId;
        private readonly Connection connection;
        private readonly SocketConfig config;
        private readonly MemoryManager memoryManager;

        internal UnreliableRawChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;
        }

        private readonly HeapMemory[] SINGLE_MESSAGE_ARRAY = new HeapMemory[1];

        public HeapMemory[] CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc)
        {
            if (payload.Count > connection.MTU)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Tried to send message that was too large. Use a fragmented channel instead. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                dealloc = false;
                headerSize = 0;
                return null;
            }

            // Set header size
            headerSize = 2;

            // Allocate the memory
            HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 2);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
            memory.Buffer[1] = channelId;

            // Copy the payload
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 2, payload.Count);

            // Tell the caller to deallc the memory
            dealloc = true;

            // Assign memory
            SINGLE_MESSAGE_ARRAY[0] = memory;

            return SINGLE_MESSAGE_ARRAY;
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Unreliable messages have no acks.
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes, out bool hasMore)
        {
            // Unreliable has one message in equal no more than one out.
            hasMore = false;

            // Set the headerBytes
            headerBytes = 0;

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
