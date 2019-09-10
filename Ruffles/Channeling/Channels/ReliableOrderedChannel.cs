using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class ReliableOrderedChannel : IChannel
    {
        private struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.isDead;

            public bool Alive;
            public ushort Sequence;
            public HeapMemory Memory;
            public DateTime LastSent;
            public DateTime FirstSent;
            public ushort Attempts;

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

        // Outgoing sequencing
        private ushort _lastOutboundSequenceNumber;
        private PendingOutgoingPacket _lastOutgoingPacket;

        // Channel info
        private readonly byte channelId;
        private readonly Connection connection;
        private readonly SocketConfig config;
        private readonly MemoryManager memoryManager;

        // Lock for the channel, this allows sends and receives being done on different threads.
        private readonly object _lock = new object();

        internal ReliableOrderedChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;

            _lastOutgoingPacket = new PendingOutgoingPacket()
            {
                Alive = false,
                Attempts = 0,
                FirstSent = DateTime.MinValue,
                LastSent = DateTime.MinValue,
                Memory = null,
                Sequence = 0
            };
        }

        private readonly HeapMemory[] SINGLE_MESSAGE_ARRAY = new HeapMemory[1];

        public object[] CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc)
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

                // Dealloc the last packet
                _lastOutgoingPacket.DeAlloc(memoryManager);

                // Add the memory to pending
                _lastOutgoingPacket = new PendingOutgoingPacket()
                {
                    Alive = true,
                    Sequence = _lastOutboundSequenceNumber,
                    Attempts = 1,
                    LastSent = DateTime.Now,
                    FirstSent = DateTime.Now,
                    Memory = memory
                };

                // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
                dealloc = false;

                // Assign memory
                SINGLE_MESSAGE_ARRAY[0] = memory;

                return SINGLE_MESSAGE_ARRAY;
            }
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            lock (_lock)
            {
                if (_lastOutgoingPacket.Alive && _lastOutgoingPacket.Sequence == sequence)
                {
                    // Add statistics
                    connection.OutgoingConfirmedPackets++;

                    // Dealloc the memory held by the last packet
                    _lastOutgoingPacket.DeAlloc(memoryManager);

                    // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                    // Get the roundtrp
                    ulong roundtrip = (ulong)Math.Round((DateTime.Now - _lastOutgoingPacket.FirstSent).TotalMilliseconds);

                    // Report to the connection
                    connection.AddRoundtripSample(roundtrip);

                    // Kill the packet
                    _lastOutgoingPacket= new PendingOutgoingPacket()
                    {
                        Alive = false,
                        Sequence = sequence
                    };

                    _incomingLowestAckedSequence = sequence;
                }
            }
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes, out bool hasMore)
        {
            // ReliableStateUpdate has one message in equal no more than one out.
            hasMore = false;

            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            // Set the headerBytes
            headerBytes = 2;

            lock (_lock)
            {
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0)
                {
                    // We have already acked this message. Ack again

                    connection.IncomingDuplicatePackets++;
                    connection.IncomingDuplicateUserBytes += (ulong)payload.Count - 2;
                    connection.IncomingDuplicateTotalBytes += (ulong)payload.Count + 2;

                    SendAck(sequence);

                    return null;
                }
                else
                {
                    // This is a future packet

                    // Add to sequencer
                    _incomingLowestAckedSequence = sequence;

                    SendAck(sequence);

                    return new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2);
                }
            }
        }



        public HeapMemory HandlePoll()
        {
            return null;
        }

        public void InternalUpdate()
        {
            lock (_lock)
            {
                if (_lastOutgoingPacket.Alive)
                {
                    if (_lastOutgoingPacket.Attempts > config.ReliabilityMaxResendAttempts)
                    {
                        // If they don't ack the message, disconnect them
                        connection.Disconnect(false);
                        return;
                    }
                    else if ((DateTime.Now - _lastOutgoingPacket.LastSent).TotalMilliseconds > connection.Roundtrip * config.ReliabilityResendRoundtripMultiplier)
                    {
                        _lastOutgoingPacket = new PendingOutgoingPacket()
                        {
                            Alive = true,
                            Attempts = (ushort)(_lastOutgoingPacket.Attempts + 1),
                            LastSent = DateTime.Now,
                            FirstSent = _lastOutgoingPacket.FirstSent,
                            Memory = _lastOutgoingPacket.Memory,
                            Sequence = _lastOutgoingPacket.Sequence
                        };

                        connection.SendRaw(new ArraySegment<byte>(_lastOutgoingPacket.Memory.Buffer, (int)_lastOutgoingPacket.Memory.VirtualOffset, (int)_lastOutgoingPacket.Memory.VirtualCount), false, 4);

                        connection.OutgoingResentPackets++;
                    }
                }
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                // Clear all incoming states
                _incomingLowestAckedSequence = 0;

                // Clear all outgoing states
                _lastOutboundSequenceNumber = 0;

                // Reset the outgoing packet
                _lastOutgoingPacket = new PendingOutgoingPacket()
                {
                    Alive = false,
                    Attempts = 0,
                    FirstSent = DateTime.MinValue,
                    LastSent = DateTime.MinValue,
                    Memory = null,
                    Sequence = 0
                };
            }
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
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 4), false, 4);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
        }
    }
}
