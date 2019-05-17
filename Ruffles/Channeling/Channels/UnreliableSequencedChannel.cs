using System;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;

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

        internal UnreliableSequencedChannel(byte channelId, Connection connection)
        {
            this.channelId = channelId;
            this.connection = connection;
        }

        public HeapMemory CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            // Increment the sequence number
            _lastOutboundSequenceNumber++;

            // Allocate the memory
            HeapMemory memory = MemoryManager.Alloc((uint)payload.Count + 4);

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

            return memory;
        }

        internal HeapMemory CreateOutgoingHeartbeatMessage()
        {
            // Increment the sequence number
            _lastOutboundSequenceNumber++;

            // Allocate the memory
            HeapMemory memory = MemoryManager.Alloc(3);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Heartbeat, false);

            // Write the sequence
            memory.Buffer[1] = (byte)_lastOutboundSequenceNumber;
            memory.Buffer[2] = (byte)(_lastOutboundSequenceNumber >> 8);

            return memory;
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Unreliable messages have no acks.
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out bool hasMore)
        {
            // UnreliableSequenced has one message in equal no more than one out.
            hasMore = false;

            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) > 0)
            {
                // Set the new sequence
                _incomingLowestAckedSequence = sequence;

                return new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2);
            }

            return null;
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
            // Clear all incoming states
            _incomingLowestAckedSequence = 0;

            // Clear all outgoing states
            _lastOutboundSequenceNumber = 0;
        }
    }
}
