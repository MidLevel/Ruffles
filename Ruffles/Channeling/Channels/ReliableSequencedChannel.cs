using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Core;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class ReliableSequencedChannel : IChannel
    {
        internal struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.isDead;

            public ushort Sequence;
            public HeapMemory Memory;
            public DateTime LastSent;
            public DateTime FirstSent;
            public ushort Attempts;
            public bool Alive;

            public void DeAlloc(MemoryManager memoryManager)
            {
                if (IsAlloced)
                {
                    memoryManager.DeAlloc(Memory);
                }
            }
        }

        internal struct PendingIncomingPacket : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.isDead;

            public ushort Sequence;
            public HeapMemory Memory;
            public bool Alive;

            public void DeAlloc(MemoryManager memoryManager)
            {
                if (IsAlloced)
                {
                    memoryManager.DeAlloc(Memory);
                }
            }
        }

        // Incoming sequencing
        private ushort _incomingLowestAckedSequence;
        private readonly HeapableSlidingWindow<PendingIncomingPacket> _receiveSequencer;

        // Outgoing sequencing
        private ushort _lastOutboundSequenceNumber;
        private readonly HeapableSlidingWindow<PendingOutgoingPacket> _sendSequencer;

        // Channel info
        private readonly byte channelId;
        private readonly ConnectionBase connection;
        private readonly SocketConfig config;
        private readonly MemoryManager memoryManager;

        internal ReliableSequencedChannel(byte channelId, ConnectionBase connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;

            // Alloc the in flight windows for receive and send
            _receiveSequencer = new HeapableSlidingWindow<PendingIncomingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
            _sendSequencer = new HeapableSlidingWindow<PendingOutgoingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
        }

        public HeapMemory HandlePoll()
        {
            if (_receiveSequencer[_incomingLowestAckedSequence + 1].Alive)
            {
                ++_incomingLowestAckedSequence;

                // HandlePoll gives the memory straight to the user, they are responsible for deallocing to prevent leaks
                HeapMemory memory = _receiveSequencer[_incomingLowestAckedSequence].Memory;

                // Kill
                _receiveSequencer[_incomingLowestAckedSequence] = new PendingIncomingPacket()
                {
                    Alive = false,
                    Sequence = 0
                };


                return memory;
            }

            return null;
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out bool hasMore)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || _receiveSequencer[sequence].Alive)
            {
                // We have already acked this message. Ack again

                SendAck(sequence);

                hasMore = false;
                return null;
            }
            else if (sequence == _incomingLowestAckedSequence + 1)
            {
                // This is the packet right after

                // If the one after is alive, we give set hasMore to true
                hasMore = _receiveSequencer[_incomingLowestAckedSequence + 2].Alive;

                _incomingLowestAckedSequence++;

                // Send ack
                SendAck(sequence);

                return new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2);
            }
            else if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) > 0 && !_receiveSequencer[sequence].Alive)
            {
                // Alloc payload plus header memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count - 2);

                // Copy the payload
                Buffer.BlockCopy(payload.Array, payload.Offset + 2, memory.Buffer, 0, payload.Count - 2);

                // Add to sequencer
                _receiveSequencer[sequence] = new PendingIncomingPacket()
                {
                    Alive = true,
                    Memory = memory,
                    Sequence = sequence
                };

                // Send ack
                SendAck(sequence);
            }

            hasMore = false;
            return null;
        }

        private readonly HeapMemory[] SINGLE_MESSAGE_ARRAY = new HeapMemory[1];

        public HeapMemory[] CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            if (payload.Count > config.MaxMessageSize)
            {
                Logging.Error("Tried to send message that was too large. Use a fragmented channel instead. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                dealloc = false;
                return null;
            }

            // Increment the sequence number
            _lastOutboundSequenceNumber++;

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

            // Add the memory to the outgoing sequencer
            _sendSequencer[_lastOutboundSequenceNumber] = (new PendingOutgoingPacket()
            {
                Alive = true,
                Attempts = 1,
                LastSent = DateTime.Now,
                FirstSent = DateTime.Now,
                Sequence = _lastOutboundSequenceNumber,
                Memory = memory
            });

            // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
            dealloc = false;

            // Assign memory
            SINGLE_MESSAGE_ARRAY[0] = memory;

            return SINGLE_MESSAGE_ARRAY;
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            if (_sendSequencer[sequence].Alive)
            {
                // Dealloc the memory held by the sequencer for the packet
                _sendSequencer[sequence].DeAlloc(memoryManager);

                // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                // Get the roundtrp
                ulong roundtrip = (ulong)Math.Round((DateTime.Now - _sendSequencer[sequence].FirstSent).TotalMilliseconds);

                // Report to the connection
                connection.AddRoundtripSample(roundtrip);

                // Kill the packet
                _sendSequencer[sequence] = new PendingOutgoingPacket()
                {
                    Alive = false,
                    Sequence = sequence
                };
            }

            for (ushort i = sequence; _sendSequencer[i].Alive; i++)
            {
                _incomingLowestAckedSequence = i;
            }
        }

        public void Reset()
        {
            // Clear all incoming states
            _receiveSequencer.Release();
            _incomingLowestAckedSequence = 0;

            // Clear all outgoing states
            _sendSequencer.Release();
            _lastOutboundSequenceNumber = 0;
        }

        private void SendAck(ushort sequence)
        {
            // Alloc ack memory
            HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

            // Write header
            ackMemory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Ack, false);
            ackMemory.Buffer[1] = (byte)channelId;

            // Write sequence
            ackMemory.Buffer[2] = (byte)sequence;
            ackMemory.Buffer[3] = (byte)(sequence >> 8);

            // Send ack
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 4), false);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
        }

        public void InternalUpdate()
        {
            long distance = SequencingUtils.Distance(_lastOutboundSequenceNumber, _incomingLowestAckedSequence, sizeof(ushort));

            for (ushort i = _incomingLowestAckedSequence; i < _incomingLowestAckedSequence + distance; i++)
            {
                if (_sendSequencer[i].Alive)
                {
                    if (_sendSequencer[i].Attempts > config.ReliabilityMaxResendAttempts)
                    {
                        // If they don't ack the message, disconnect them
                        connection.Disconnect(false);
                    }
                    else if ((DateTime.Now - _sendSequencer[i].LastSent).TotalMilliseconds > connection.Roundtrip * config.ReliabilityResendRoundtripMultiplier)
                    {
                        _sendSequencer[i] = new PendingOutgoingPacket()
                        {
                            Alive = true,
                            Attempts = (ushort)(_sendSequencer[i].Attempts + 1),
                            LastSent = DateTime.Now,
                            FirstSent = _sendSequencer[i].FirstSent,
                            Memory = _sendSequencer[i].Memory,
                            Sequence = i
                        };

                        connection.SendRaw(new ArraySegment<byte>(_sendSequencer[i].Memory.Buffer, (int)_sendSequencer[i].Memory.VirtualOffset, (int)_sendSequencer[i].Memory.VirtualCount), false);
                    }
                }
            }
        }
    }
}
 