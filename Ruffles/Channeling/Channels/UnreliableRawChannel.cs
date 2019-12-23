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
        private byte channelId;
        private Connection connection;
        private SocketConfig config;
        private MemoryManager memoryManager;

        internal UnreliableRawChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;
        }

        public HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            if (payload.Count > connection.MTU)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Tried to send message that was too large. Use a fragmented channel instead. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                dealloc = false;
                return null;
            }

            // Allocate the memory
            HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 2);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack(MessageType.Data);
            memory.Buffer[1] = channelId;

            // Copy the payload
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 2, payload.Count);

            // Tell the caller to deallc the memory
            dealloc = true;

            // Allocate pointers
            HeapPointers pointers = memoryManager.AllocHeapPointers(1);

            // Point the first pointer to the memory
            pointers.Pointers[pointers.VirtualOffset] = memory;

            return pointers;
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Unreliable messages have no acks.
        }

        public HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload)
        {
            // Alloc pointers
            HeapPointers pointers = memoryManager.AllocHeapPointers(1);

            // Alloc wrapper
            pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(new ArraySegment<byte>(payload.Array, payload.Offset, payload.Count));

            return pointers;
        }

        public void InternalUpdate()
        {
            // Unreliable doesnt need to resend, thus no internal loop is required
        }

        public void Release()
        {
            // UnreliableRaw has nothing to clean up
        }

        public void Assign(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;
        }
    }
}
