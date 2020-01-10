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
    internal class ReliableOrderedChannel : IChannel
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
        private ushort _incomingLowestAckedSequence;
        private readonly SlidingWindow<NetTime> _lastAckTimes;
        private readonly object _receiveLock = new object();

        // Outgoing sequencing
        private ushort _lastOutgoingSequence;
        private PendingOutgoingPacket _lastOutgoingPacket;
        private readonly object _sendLock = new object();

        // Channel info
        private byte channelId;
        private Connection connection;
        private SocketConfig config;
        private MemoryManager memoryManager;

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
                FirstSent = NetTime.MinValue,
                LastSent = NetTime.MinValue,
                Memory = null,
                Sequence = 0
            };

            _lastAckTimes = new SlidingWindow<NetTime>(config.ReliableAckFlowWindowSize);
        }

        public void CreateOutgoingMessage(ArraySegment<byte> payload, bool noMerge)
        {
            if (payload.Count > connection.MTU)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Tried to send message that was too large. Use a fragmented channel instead. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                return;
            }

            lock (_sendLock)
            {
                // Increment the sequence number
                _lastOutgoingSequence++;

                // Allocate the memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 4);

                // Write headers
                memory.Buffer[0] = HeaderPacker.Pack(MessageType.Data);
                memory.Buffer[1] = channelId;

                // Write the sequence
                memory.Buffer[2] = (byte)_lastOutgoingSequence;
                memory.Buffer[3] = (byte)(_lastOutgoingSequence >> 8);

                // Copy the payload
                Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 4, payload.Count);

                // Dealloc the last packet
                _lastOutgoingPacket.DeAlloc(memoryManager);

                // Add the memory to pending
                _lastOutgoingPacket = new PendingOutgoingPacket()
                {
                    Alive = true,
                    Sequence = _lastOutgoingSequence,
                    Attempts = 1,
                    LastSent = NetTime.Now,
                    FirstSent = NetTime.Now,
                    Memory = memory
                };

                // Allocate pointers
                HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                // Point the first pointer to the memory
                pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(memory);

                // Send the message to the router. Tell the router to NOT dealloc the memory as the channel needs it for resend purposes.
                ChannelRouter.SendMessage(pointers, false, connection, noMerge, memoryManager);
            }
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            lock (_sendLock)
            {
                if (_lastOutgoingPacket.Alive && _lastOutgoingPacket.Sequence == sequence)
                {
                    // Dealloc the memory held by the last packet
                    _lastOutgoingPacket.DeAlloc(memoryManager);

                    // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                    // Get the roundtrp
                    ulong roundtrip = (ulong)Math.Round((NetTime.Now - _lastOutgoingPacket.FirstSent).TotalMilliseconds);

                    // Report to the connection
                    connection.AddRoundtripSample(roundtrip);

                    // Kill the packet
                    _lastOutgoingPacket = new PendingOutgoingPacket()
                    {
                        Alive = false,
                        Sequence = sequence
                    };
                }
            }
        }

        public HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));

            lock (_receiveLock)
            {
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0)
                {
                    // We have already acked this message. Ack again

                    SendAck(sequence);

                    return null;
                }
                else
                {
                    // This is a future packet

                    // Add to sequencer
                    _incomingLowestAckedSequence = sequence;

                    // Send ack
                    SendAck(sequence);

                    // Alloc pointers
                    HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                    // Alloc a memory wrapper
                    pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2));

                    return pointers;
                }
            }
        }

        public void InternalUpdate(out bool timeout)
        {
            lock (_sendLock)
            {
                if (_lastOutgoingPacket.Alive)
                {
                    if ((NetTime.Now - _lastOutgoingPacket.LastSent).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - _lastOutgoingPacket.LastSent).TotalMilliseconds > config.ReliabilityMinPacketResendDelay)
                    {
                        if (_lastOutgoingPacket.Attempts > config.ReliabilityMaxResendAttempts)
                        {
                            // If they don't ack the message, disconnect them
                            timeout = true;
                            return;
                        }

                        _lastOutgoingPacket = new PendingOutgoingPacket()
                        {
                            Alive = true,
                            Attempts = (ushort)(_lastOutgoingPacket.Attempts + 1),
                            LastSent = NetTime.Now,
                            FirstSent = _lastOutgoingPacket.FirstSent,
                            Memory = _lastOutgoingPacket.Memory,
                            Sequence = _lastOutgoingPacket.Sequence
                        };

                        connection.SendInternal(new ArraySegment<byte>(_lastOutgoingPacket.Memory.Buffer, (int)_lastOutgoingPacket.Memory.VirtualOffset, (int)_lastOutgoingPacket.Memory.VirtualCount), false);
                    }
                }
            }

            timeout = false;
        }

        private void SendAck(ushort sequence)
        {
            // Check the last ack time
            if (!_lastAckTimes.TryGet(sequence, out NetTime value) || ((NetTime.Now - value).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - value).TotalMilliseconds > config.ReliabilityMinAckResendDelay))
            {
                // Set the last ack time
                _lastAckTimes.Set(sequence, NetTime.Now);

                // Alloc ack memory
                HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

                // Write header
                ackMemory.Buffer[0] = HeaderPacker.Pack(MessageType.Ack);
                ackMemory.Buffer[1] = (byte)channelId;

                // Write sequence
                ackMemory.Buffer[2] = (byte)sequence;
                ackMemory.Buffer[3] = (byte)(sequence >> 8);

                // Send ack
                connection.SendInternal(new ArraySegment<byte>(ackMemory.Buffer, 0, 4), false);

                // Return memory
                memoryManager.DeAlloc(ackMemory);
            }
        }

        public void Release()
        {
            lock (_sendLock)
            {
                lock (_receiveLock)
                {
                    // Clear all incoming states
                    _incomingLowestAckedSequence = 0;

                    // Clear all outgoing states
                    _lastOutgoingSequence = 0;

                    // Reset the outgoing packet
                    _lastOutgoingPacket = new PendingOutgoingPacket()
                    {
                        Alive = false,
                        Attempts = 0,
                        FirstSent = NetTime.MinValue,
                        LastSent = NetTime.MinValue,
                        Memory = null,
                        Sequence = 0
                    };
                }
            }
        }

        public void Assign(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            lock (_sendLock)
            {
                lock (_receiveLock)
                {
                    this.channelId = channelId;
                    this.connection = connection;
                    this.config = config;
                    this.memoryManager = memoryManager;
                }
            }
        }
    }
}
