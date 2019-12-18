using System;
using System.Collections.Generic;
using Ruffles.Collections;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Time;
using Ruffles.Utils;

namespace Ruffles.Channeling.Channels
{
    internal class ReliableChannel : IChannel
    {
        private struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.IsDead;

            public bool Alive;
            public ushort Sequence;
            public HeapMemory Memory;
            public NetTime LastSent;
            public NetTime FirstSent;
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
        private readonly HashSet<ushort> _incomingAckedSequences = new HashSet<ushort>();
        private ushort _incomingLowestAckedSequence;
        private readonly SlidingWindow<NetTime> _lastAckTimes;


        // Outgoing sequencing
        private ushort _lastOutgoingSequence;
        private ushort _outgoingLowestAckedSequence;
        private readonly HeapableSlidingWindow<PendingOutgoingPacket> _sendSequencer;

        // Channel info
        private byte channelId;
        private Connection connection;
        private SocketConfig config;
        private MemoryManager memoryManager;

        // Lock for the channel, this allows sends and receives being done on different threads.
        private readonly object _lock = new object();

        internal ReliableChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;

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
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || _incomingAckedSequences.Contains(sequence))
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
                    // This is the "next" packet

                    do
                    {
                        // Remove previous
                        _incomingAckedSequences.Remove(_incomingLowestAckedSequence);

                        _incomingLowestAckedSequence++;
                    }
                    while (_incomingAckedSequences.Contains((ushort)(_incomingLowestAckedSequence + 1)));

                    // Ack the new message
                    SendAck(sequence);


                    // Alloc pointers
                    HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                    // Alloc a memory wrapper
                    pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2));

                    return pointers;
                }
                else if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) > 0 && !_incomingAckedSequences.Contains(sequence))
                {
                    // This is a future packet

                    // Add to sequencer
                    _incomingAckedSequences.Add(sequence);

                    // Ack the new message
                    SendAck(sequence);


                    // Alloc pointers
                    HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                    // Alloc a memory wrapper
                    pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2));

                    return pointers;
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
                PendingOutgoingPacket unsafeOutgoing = _sendSequencer.GetUnsafe(_lastOutgoingSequence + 1, out bool isSafe);

                if (unsafeOutgoing.Alive && !isSafe)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Outgoing packet window is exhausted. Disconnecting");

                    connection.Disconnect(false);

                    dealloc = false;
                    headerSize = 0;
                    return null;
                }

                // Increment the sequence number
                _lastOutgoingSequence++;

                // Set header size
                headerSize = 4;

                // Allocate the memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 4);

                // Write headers
                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
                memory.Buffer[1] = channelId;

                // Write the sequence
                memory.Buffer[2] = (byte)_lastOutgoingSequence;
                memory.Buffer[3] = (byte)(_lastOutgoingSequence >> 8);

                // Copy the payload
                Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 4, payload.Count);

                // Add the memory to pending
                _sendSequencer[_lastOutgoingSequence] = new PendingOutgoingPacket()
                {
                    Alive = true,
                    Sequence = _lastOutgoingSequence,
                    Attempts = 1,
                    LastSent = NetTime.Now,
                    FirstSent = NetTime.Now,
                    Memory = memory
                };

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

                    // Dealloc the memory held by the sequencer
                    memoryManager.DeAlloc(_sendSequencer[sequence].Memory);

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

                    if (sequence == (ushort)(_outgoingLowestAckedSequence + 1))
                    {
                        // This was the next one.
                        _outgoingLowestAckedSequence++;
                    }
                }

                // Loop from the lowest ack we got
                for (ushort i = _outgoingLowestAckedSequence; !_sendSequencer[i].Alive && SequencingUtils.Distance(i, _lastOutgoingSequence, sizeof(ushort)) <= 0; i++)
                {
                    _outgoingLowestAckedSequence = i;
                }
            }
        }

        public void InternalUpdate()
        {
            lock (_lock)
            {
                for (ushort i = (ushort)(_outgoingLowestAckedSequence + 1); SequencingUtils.Distance(i, _lastOutgoingSequence, sizeof(ushort)) < 0; i++)
                {
                    if (_sendSequencer[i].Alive)
                    {
                        if ((NetTime.Now - _sendSequencer[i].LastSent).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - _sendSequencer[i].LastSent).TotalMilliseconds > config.ReliabilityMinPacketResendDelay)
                        {
                            if (_sendSequencer[i].Attempts > config.ReliabilityMaxResendAttempts)
                            {
                                // If they don't ack the message, disconnect them
                                connection.Disconnect(false);
                                return;
                            }

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
                        bool bitAcked = SequencingUtils.Distance(bitSequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || _incomingAckedSequences.Contains(bitSequence);

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

        public void Release()
        {
            lock (_lock)
            {
                // Clear all incoming states
                _incomingAckedSequences.Clear();
                _incomingLowestAckedSequence = 0;

                // Clear all outgoing states
                _sendSequencer.Release();
                _lastOutgoingSequence = 0;
                _outgoingLowestAckedSequence = 0;
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
