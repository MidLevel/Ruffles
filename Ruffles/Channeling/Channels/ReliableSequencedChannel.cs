using System;
using Ruffles.Collections;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Time;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class ReliableSequencedChannel : IChannel
    {
        internal struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.IsDead;

            public ushort Sequence;
            public HeapMemory Memory;
            public NetTime LastSent;
            public NetTime FirstSent;
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
            public bool IsAlloced => Memory != null && !Memory.IsDead;

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
        private readonly SlidingWindow<NetTime> _lastAckTimes;

        // Outgoing sequencing
        private ushort _lastOutboundSequenceNumber;
        private readonly HeapableSlidingWindow<PendingOutgoingPacket> _sendSequencer;

        // Channel info
        private byte channelId;
        private Connection connection;
        private SocketConfig config;
        private MemoryManager memoryManager;

        // Lock for the channel, this allows sends and receives being done on different threads.
        private readonly object _lock = new object();

        internal ReliableSequencedChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;

            // Alloc the in flight windows for receive and send
            _receiveSequencer = new HeapableSlidingWindow<PendingIncomingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
            _sendSequencer = new HeapableSlidingWindow<PendingOutgoingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
            _lastAckTimes = new SlidingWindow<NetTime>(config.ReliableAckFlowWindowSize, true, sizeof(ushort));
        }

        public HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            // Set the headerBytes
            headerBytes = 2;

            lock (_lock)
            {
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || _receiveSequencer[sequence].Alive)
                {
                    // We have already acked this message. Ack again

                    connection.IncomingDuplicatePackets++;
                    connection.IncomingDuplicateUserBytes += (ulong)payload.Count - 2;
                    connection.IncomingDuplicateTotalBytes += (ulong)payload.Count + 2;

                    SendAck(sequence);

                    return null;
                }
                else if (sequence == _incomingLowestAckedSequence + 1)
                {
                    // This is the packet right after

                    _incomingLowestAckedSequence++;

                    // Send ack
                    SendAck(sequence);

                    uint additionalPackets = 0;

                    // Count all the additional sequential packets that are ready
                    for (int i = 1; _receiveSequencer[_incomingLowestAckedSequence + i].Alive; i++)
                    {
                        additionalPackets++;
                    }

                    // Allocate pointers (alloc size 1, we might need more)
                    HeapPointers pointers = memoryManager.AllocHeapPointers(1 + additionalPackets);

                    // Point the first pointer to the memory that is known.
                    pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2));

                    for (int i = 0; _receiveSequencer[_incomingLowestAckedSequence + 1].Alive; i++)
                    {
                        // Update lowest incoming
                        ++_incomingLowestAckedSequence;

                        // Set the memory
                        pointers.Pointers[1 + i] = memoryManager.AllocMemoryWrapper(_receiveSequencer[_incomingLowestAckedSequence].Memory);

                        // Kill
                        _receiveSequencer[_incomingLowestAckedSequence] = new PendingIncomingPacket()
                        {
                            Alive = false,
                            Sequence = 0
                        };
                    }

                    return pointers;

                }
                else if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) > 0)
                {
                    // Future packet

                    PendingIncomingPacket unsafeIncoming = _receiveSequencer.GetUnsafe(sequence, out bool isSafe);

                    if (unsafeIncoming.Alive && !isSafe)
                    {
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Incoming packet window is exhausted. Expect delays");

                        return null;
                    }
                    else if (!_receiveSequencer[sequence].Alive)
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
                }

                return null;
            }
        }

        public HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc)
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
                PendingOutgoingPacket unsafeOutgoing = _sendSequencer.GetUnsafe(_lastOutboundSequenceNumber + 1, out bool isSafe);

                if (unsafeOutgoing.Alive && !isSafe)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Outgoing packet window is exhausted. Disconnecting");

                    connection.Disconnect(false);

                    dealloc = false;
                    headerSize = 0;
                    return null;
                }

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

                // Add the memory to the outgoing sequencer
                _sendSequencer[_lastOutboundSequenceNumber] = (new PendingOutgoingPacket()
                {
                    Alive = true,
                    Attempts = 1,
                    LastSent = NetTime.Now,
                    FirstSent = NetTime.Now,
                    Sequence = _lastOutboundSequenceNumber,
                    Memory = memory
                });

                // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
                dealloc = false;

                // Allocate pointers
                HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                // Point the first pointer to the memory
                pointers.Pointers[pointers.VirtualOffset] = memory;

                return pointers;
            }
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            // Handle the base ack
            HandleAck(sequence);

            if ((payload.Count - 2) > 0)
            {
                // There is more data. This has to be ack bits

                // Calculate the amount of ack bits
                int bits = (payload.Count - 2) * 8;

                // Iterate ack bits
                for (byte i = 0; i < bits; i++)
                {
                    // Get the ack for the current bit
                    bool isAcked = ((payload.Array[payload.Offset + 2 + (i / 8)] & ((byte)Math.Pow(2, (7 - (i % 8))))) >> (7 - (i % 8))) == 1;

                    if (isAcked)
                    {
                        // Handle the bit ack
                        HandleAck((ushort)(sequence - (i + 1)));
                    }
                }
            }
        }

        private void HandleAck(ushort sequence)
        {
            lock (_lock)
            {
                if (_sendSequencer[sequence].Alive)
                {
                    // Add statistics
                    connection.OutgoingConfirmedPackets++;

                    // Dealloc the memory held by the sequencer for the packet
                    _sendSequencer[sequence].DeAlloc(memoryManager);

                    // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                    // Get the roundtrp
                    ulong roundtrip = (ulong)Math.Round((NetTime.Now - _sendSequencer[sequence].FirstSent).TotalMilliseconds);

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
        }

        private void SendAck(ushort sequence)
        {
            // Check the last ack time
            if ((NetTime.Now - _lastAckTimes[sequence]).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - _lastAckTimes[sequence]).TotalMilliseconds > config.ReliabilityMinAckResendDelay)
            {
                // Set the last ack time
                _lastAckTimes[sequence] = NetTime.Now;

                // Alloc ack memory
                HeapMemory ackMemory = memoryManager.AllocHeapMemory(4 + (uint)(config.EnableMergedAcks ? config.MergedAckBytes : 0));

                // Write header
                ackMemory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Ack, false);
                ackMemory.Buffer[1] = (byte)channelId;

                // Write sequence
                ackMemory.Buffer[2] = (byte)sequence;
                ackMemory.Buffer[3] = (byte)(sequence >> 8);

                if (config.EnableMergedAcks)
                {
                    // Reset the memory
                    for (int i = 0; i < config.MergedAckBytes; i++)
                    {
                        ackMemory.Buffer[4 + i] = 0;
                    }

                    // Set the bit fields
                    for (int i = 0; i < config.MergedAckBytes * 8; i++)
                    {
                        ushort bitSequence = (ushort)(sequence - (i + 1));
                        bool bitAcked = SequencingUtils.Distance(bitSequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || _receiveSequencer[bitSequence].Alive;

                        if (bitAcked)
                        {
                            // Set the ack time for this packet
                            _lastAckTimes[sequence] = NetTime.Now;
                        }

                        // Write single ack bit
                        ackMemory.Buffer[4 + (i / 8)] |= (byte)((bitAcked ? 1 : 0) << (7 - (i % 8)));
                    }
                }

                // Send ack
                connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 4 + (config.EnableMergedAcks ? config.MergedAckBytes : 0)), false, (byte)(4 + (config.EnableMergedAcks ? config.MergedAckBytes : 0)));

                // Return memory
                memoryManager.DeAlloc(ackMemory);
            }
        }

        public void InternalUpdate()
        {
            lock (_lock)
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
                            return;
                        }
                        else if ((NetTime.Now - _sendSequencer[i].LastSent).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - _sendSequencer[i].LastSent).TotalMilliseconds > config.ReliabilityMinPacketResendDelay)
                        {
                            _sendSequencer[i] = new PendingOutgoingPacket()
                            {
                                Alive = true,
                                Attempts = (ushort)(_sendSequencer[i].Attempts + 1),
                                LastSent = NetTime.Now,
                                FirstSent = _sendSequencer[i].FirstSent,
                                Memory = _sendSequencer[i].Memory,
                                Sequence = i
                            };

                            connection.SendRaw(new ArraySegment<byte>(_sendSequencer[i].Memory.Buffer, (int)_sendSequencer[i].Memory.VirtualOffset, (int)_sendSequencer[i].Memory.VirtualCount), false, 4);

                            connection.OutgoingResentPackets++;
                        }
                    }
                }
            }
        }

        public void Release()
        {
            lock (_lock)
            {
                // Clear all incoming states
                _receiveSequencer.Release();
                _incomingLowestAckedSequence = 0;

                // Clear all outgoing states
                _sendSequencer.Release();
                _lastOutboundSequenceNumber = 0;
            }
        }

        public void Assign(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            lock (_lock)
            {
                this.channelId = channelId;
                this.connection = connection;
                this.config = config;
                this.memoryManager = memoryManager;
            }
        }
    }
}
 