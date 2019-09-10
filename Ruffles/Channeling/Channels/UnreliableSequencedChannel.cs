using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class UnreliableSequencedChannel : IChannel
    {
        // Incoming sequencing
        private ushort _incomingLowestAckedSequence;

        // Outgoing sequencing
        private ushort _lastOutboundSequenceNumber;

        // Channel info
        private readonly byte channelId;
        private readonly Connection connection;
        private readonly MemoryManager memoryManager;
        private readonly SocketConfig config;

        // Lock for the channel, this allows sends and receives being done on different threads.
        private readonly object _lock = new object();

        internal UnreliableSequencedChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.memoryManager = memoryManager;
            this.config = config;
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

            lock (_lock)
            {
                // Increment the sequence number
                _lastOutboundSequenceNumber++;

                // Set header size
                headerSize = 4;

                // Allocate the memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 4);

                // Write headers
                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
                memory.Buffer[1] = channelId;

                // Write the sequence
                memory.Buffer[2] = (byte)_lastOutboundSequenceNumber;
                memory.Buffer[3] = (byte)(_lastOutboundSequenceNumber >> 8);

                // Copy the payload
                Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 4, payload.Count);

                // Tell the caller to deallc the memory
                dealloc = true;

                // Assign memory
                SINGLE_MESSAGE_ARRAY[0] = memory;

                return SINGLE_MESSAGE_ARRAY;
            }
        }

        internal HeapMemory CreateOutgoingHeartbeatMessage()
        {
            lock (_lock)
            {
                // Increment the sequence number
                _lastOutboundSequenceNumber++;

                // Allocate the memory
                HeapMemory memory = memoryManager.AllocHeapMemory(3);

                // Write headers
                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Heartbeat, false);

                // Write the sequence
                memory.Buffer[1] = (byte)_lastOutboundSequenceNumber;
                memory.Buffer[2] = (byte)(_lastOutboundSequenceNumber >> 8);

                return memory;
            }
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Unreliable messages have no acks.
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes, out bool hasMore)
        {
            // UnreliableSequenced has one message in equal no more than one out.
            hasMore = false;

            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            // Set the headerBytes
            headerBytes = 2;

            lock (_lock)
            {
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) > 0)
                {
                    // Set the new sequence
                    _incomingLowestAckedSequence = sequence;

                    return new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2);
                }

                return null;
            }
        }

        public HeapMemory HandlePoll()
        {
            return null;
        }

        public void InternalUpdate()
        {
            // UnreliableSequenced doesnt need to resend, thus no internal loop is required
        }

        public void Reset()
        {
            lock (_lock)
            {
                // Clear all incoming states
                _incomingLowestAckedSequence = 0;

                // Clear all outgoing states
                _lastOutboundSequenceNumber = 0;
            }
        }
    }
}
