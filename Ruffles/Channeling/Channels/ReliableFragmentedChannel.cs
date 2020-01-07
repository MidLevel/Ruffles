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
    // TODO: Make single fragment messages faster
    // TODO: Remove array allocs
    internal class ReliableFragmentedChannel : IChannel
    {
        internal struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced
            {
                get
                {
                    return Fragments != null;
                }
            }

            public HeapPointers Fragments;
            public bool Alive;

            public bool AllFragmentsAlive
            {
                get
                {
                    if (Fragments == null)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.VirtualCount; i++)
                    {
                        if (!((PendingOutgoingFragment)Fragments.Pointers[Fragments.VirtualOffset + i]).Alive)
                        {
                            return false;
                        }
                    }

                    return true;
                }
            }

            public void DeAlloc(MemoryManager memoryManager)
            {
                if (IsAlloced)
                {
                    for (int i = 0; i < Fragments.VirtualCount; i++)
                    {
                        if (((PendingOutgoingFragment)Fragments.Pointers[Fragments.VirtualOffset + i]).Alive && ((PendingOutgoingFragment)Fragments.Pointers[Fragments.VirtualOffset + i]).IsAlloced)
                        {
                            ((PendingOutgoingFragment)Fragments.Pointers[Fragments.VirtualOffset + i]).DeAlloc(memoryManager);
                        }
                    }

                    // Dealloc the pointers
                    memoryManager.DeAlloc(Fragments);
                }
            }
        }


        internal struct PendingOutgoingFragment : IMemoryReleasable
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
            public bool IsAlloced
            {
                get
                {
                    return Fragments != null;
                }
            }

            public bool IsComplete
            {
                get
                {
                    if (Fragments == null || Size == null || Fragments.VirtualCount < Size)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.VirtualCount; i++)
                    {
                        if (Fragments.Pointers[Fragments.VirtualOffset + i] == null || ((HeapMemory)Fragments.Pointers[Fragments.VirtualOffset + i]).IsDead)
                        {
                            return false;
                        }
                    }

                    return true;
                }
            }

            public uint TotalByteSize
            {
                get
                {
                    uint byteSize = 0;

                    if (!IsComplete)
                    {
                        // TODO: Throw
                        return byteSize;
                    }


                    for (int i = 0; i < Fragments.VirtualCount; i++)
                    {
                        byteSize += ((HeapMemory)Fragments.Pointers[Fragments.VirtualOffset + i]).VirtualCount;
                    }

                    return byteSize;
                }
            }

            public ushort Sequence;
            public ushort? Size;
            public HeapPointers Fragments;
            public bool Alive;

            public void DeAlloc(MemoryManager memoryManager)
            {
                if (IsAlloced)
                {
                    for (int i = 0; i < Fragments.VirtualCount; i++)
                    {
                        if (Fragments.Pointers[Fragments.VirtualOffset + i] != null && !((HeapMemory)Fragments.Pointers[Fragments.VirtualOffset + i]).IsDead)
                        {
                            memoryManager.DeAlloc((HeapMemory)Fragments.Pointers[Fragments.VirtualOffset + i]);
                        }
                    }

                    memoryManager.DeAlloc(Fragments);
                }
            }
        }

        // Incoming sequencing
        private readonly HashSet<ushort> _incomingAckedSequences = new HashSet<ushort>();
        private ushort _incomingLowestAckedSequence;
        private readonly HeapableSlidingWindow<PendingIncomingPacket> _receiveSequencer;

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

        internal ReliableFragmentedChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.config = config;
            this.memoryManager = memoryManager;

            // Alloc the in flight windows for receive and send
            _receiveSequencer = new HeapableSlidingWindow<PendingIncomingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
            _sendSequencer = new HeapableSlidingWindow<PendingOutgoingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort), memoryManager);
        }

        public HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));
            // Read the raw fragment data
            ushort encodedFragment = (ushort)(payload.Array[payload.Offset + 2] | (ushort)(payload.Array[payload.Offset + 3] << 8));
            // The fragmentId is the last 15 least significant bits
            ushort fragment = (ushort)(encodedFragment & 32767);
            // IsFinal is the most significant bit
            bool isFinal = (ushort)((encodedFragment & 32768) >> 15) == 1;

            if (fragment > config.MaxFragments)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("FragmentId was too large. [FragmentId=" + fragment + "] [Config.MaxFragments=" + config.MaxFragments + "]. The fragment was silently dropped, expect a timeout.");
                return null;
            }

            lock (_lock)
            {
                // If the sequence is older than the last one we sent to user OR the packet is already acked OR the sequence is alive and its complete OR (the sequence is alive AND the fragments is alloced AND the alloced fragment count is larger than the fragment (I.E, the fragment is actually alloced) AND the fragment is not null AND the fragment is not dead))
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 ||
                    (_incomingAckedSequences.Contains(sequence)) ||
                    (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].IsComplete) ||
                    (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].Fragments != null && _receiveSequencer[sequence].Fragments.VirtualCount > fragment && _receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + fragment] != null && !((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + fragment]).IsDead))
                {
                    // We have already acked this message. Ack again

                    SendAckEncoded(sequence, encodedFragment);

                    return null;
                }
                else
                {
                    // This is a packet after the last. One that is not yet completed

                    PendingIncomingPacket unsafeIncoming = _receiveSequencer.GetUnsafe(sequence, out bool isSafe);

                    if (unsafeIncoming.Alive && !isSafe)
                    {
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Incoming packet window is exhausted. Expect delays");

                        return null;
                    }
                    else if (!_receiveSequencer[sequence].Alive)
                    {
                        // If this is the first fragment we ever get, index the data.

                        HeapPointers fragmentPointers = memoryManager.AllocHeapPointers((uint)fragment + 1);

                        _receiveSequencer[sequence] = new PendingIncomingPacket()
                        {
                            Alive = true,
                            Fragments = fragmentPointers,
                            Sequence = sequence,
                            Size = isFinal ? (ushort?)(fragment + 1) : null
                        };
                    }
                    else
                    {
                        // If the first fragment we got was fragment 1 / 500. The fragments array will only be of size 128. We need to potentially resize it
                        if (_receiveSequencer[sequence].Fragments.Pointers.Length - _receiveSequencer[sequence].Fragments.VirtualOffset <= fragment)
                        {
                            // We need to expand the fragments array.

                            // Alloc new array
                            HeapPointers newPointers = memoryManager.AllocHeapPointers((uint)fragment + 1);

                            // Copy old values
                            Array.Copy(_receiveSequencer[sequence].Fragments.Pointers, newPointers.Pointers, _receiveSequencer[sequence].Fragments.Pointers.Length);

                            // Return the memory for the old
                            memoryManager.DeAlloc(_receiveSequencer[sequence].Fragments);

                            // Update the index
                            _receiveSequencer[sequence] = new PendingIncomingPacket()
                            {
                                Fragments = newPointers,
                                Alive = _receiveSequencer[sequence].Alive,
                                Sequence = _receiveSequencer[sequence].Sequence,
                                Size = isFinal ? (ushort?)(fragment + 1) : _receiveSequencer[sequence].Size
                            };
                        }

                        // We might also have to expand the virtual count
                        if (_receiveSequencer[sequence].Fragments.VirtualCount <= fragment)
                        {
                            // Update the new virtual count
                            _receiveSequencer[sequence].Fragments.VirtualCount = (uint)fragment + 1;

                            // Update the struct to set the size if it has changed (TODO: Check if needed)
                            _receiveSequencer[sequence] = new PendingIncomingPacket()
                            {
                                Fragments = _receiveSequencer[sequence].Fragments,
                                Alive = _receiveSequencer[sequence].Alive,
                                Sequence = _receiveSequencer[sequence].Sequence,
                                Size = isFinal ? (ushort?)(fragment + 1) : _receiveSequencer[sequence].Size
                            };
                        }
                    }

                    // If the fragment is null OR the fragment is DEAD
                    if (_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + fragment] == null || ((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + fragment]).IsDead)
                    {
                        // Alloc some memory for the fragment
                        HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count - 4);

                        // Copy the payload
                        Buffer.BlockCopy(payload.Array, payload.Offset + 4, memory.Buffer, 0, payload.Count - 4);

                        // Add fragment to index
                        _receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + fragment] = memory;
                    }

                    // Send ack
                    SendAckEncoded(sequence, encodedFragment);

                    // If this sequence just completed. Return the memory
                    if (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].IsComplete)
                    {
                        if (sequence == (ushort)(_incomingLowestAckedSequence + 1))
                        {
                            // This is the next packet.

                            do
                            {
                                // Remove previous
                                _incomingAckedSequences.Remove(_incomingLowestAckedSequence);

                                _incomingLowestAckedSequence++;
                            }
                            while (_incomingAckedSequences.Contains((ushort)(_incomingLowestAckedSequence + 1)));
                        }
                        else
                        {
                            // This is a future one
                            _incomingAckedSequences.Add(sequence);
                        }

                        // Get the total size of all fragments
                        uint totalSize = _receiveSequencer[sequence].TotalByteSize;

                        // Alloc memory for that large segment
                        HeapMemory memory = memoryManager.AllocHeapMemory(totalSize);

                        // Keep track of where we are, fragments COULD have different sizes.
                        int bufferPosition = 0;

                        if (_receiveSequencer[sequence].Fragments != null)
                        {
                            // Copy all the parts
                            for (int i = 0; i < _receiveSequencer[sequence].Fragments.VirtualCount; i++)
                            {
                                // Copy fragment to final buffer
                                Buffer.BlockCopy(((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + i]).Buffer, (int)((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + i]).VirtualOffset, memory.Buffer, bufferPosition, (int)((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + i]).VirtualCount);

                                bufferPosition += (int)((HeapMemory)_receiveSequencer[sequence].Fragments.Pointers[_receiveSequencer[sequence].Fragments.VirtualOffset + i]).VirtualCount;
                            }
                        }

                        // Free the memory of all the individual fragments
                        _receiveSequencer[sequence].DeAlloc(memoryManager);

                        // Kill
                        _receiveSequencer[sequence] = new PendingIncomingPacket()
                        {
                            Alive = false,
                            Sequence = 0,
                            Size = null,
                            Fragments = null
                        };


                        // Alloc pointers
                        HeapPointers pointers = memoryManager.AllocHeapPointers(1);

                        // Alloc a memory wrapper
                        pointers.Pointers[0] = memoryManager.AllocMemoryWrapper(memory);

                        return pointers;
                    }

                    return null;
                }
            }
        }

        public HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            // Calculate the amount of fragments required
            int fragmentsRequired = (payload.Count + (connection.MTU - 1)) / connection.MTU;

            if (fragmentsRequired > config.MaxFragments)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Tried to create message that was too large. [Size=" + payload.Count + "] [FragmentsRequired=" + fragmentsRequired + "] [Config.MaxFragments=" + config.MaxFragments + "]");
                dealloc = false;
                return null;
            }

            lock (_lock)
            {
                PendingOutgoingPacket unsafeOutgoing = _sendSequencer.GetUnsafe((ushort)(_lastOutgoingSequence + 1), out bool isSafe);

                if (unsafeOutgoing.Alive && !isSafe)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Outgoing packet window is exhausted. Packet lost");

                    dealloc = false;
                    return null;
                }

                // Increment the sequence number
                _lastOutgoingSequence++;

                // Alloc array
                HeapPointers memoryParts = memoryManager.AllocHeapPointers((uint)fragmentsRequired);

                int position = 0;

                for (ushort i = 0; i < fragmentsRequired; i++)
                {
                    // Calculate message size
                    int messageSize = Math.Min(connection.MTU, payload.Count - position);

                    // Allocate memory for each fragment
                    memoryParts.Pointers[memoryParts.VirtualOffset + i] = memoryManager.AllocHeapMemory((uint)(messageSize + 6));

                    // Write headers
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[0] = HeaderPacker.Pack(MessageType.Data);
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[1] = channelId;

                    // Write the sequence
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[2] = (byte)_lastOutgoingSequence;
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[3] = (byte)(_lastOutgoingSequence >> 8);

                    // Write the fragment
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[4] = (byte)(i & 32767);
                    ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer[5] = (byte)(((i & 32767) >> 8) | (byte)(i == fragmentsRequired - 1 ? 128 : 0));

                    // Write the payload
                    Buffer.BlockCopy(payload.Array, payload.Offset + position, ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i]).Buffer, 6, messageSize);

                    // Increase the position
                    position += messageSize;
                }

                // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
                dealloc = false;

                // Alloc outgoing fragment structs
                HeapPointers outgoingFragments = memoryManager.AllocHeapPointers((uint)fragmentsRequired);

                for (int i = 0; i < fragmentsRequired; i++)
                {
                    // Add the memory to the outgoing sequencer array
                    outgoingFragments.Pointers[outgoingFragments.VirtualOffset + i] = new PendingOutgoingFragment()
                    {
                        Alive = true,
                        Attempts = 1,
                        LastSent = NetTime.Now,
                        FirstSent = NetTime.Now,
                        Sequence = _lastOutgoingSequence,
                        Memory = ((HeapMemory)memoryParts.Pointers[memoryParts.VirtualOffset + i])
                    };
                }

                // Add the memory to the outgoing sequencer
                _sendSequencer[_lastOutgoingSequence] = new PendingOutgoingPacket()
                {
                    Alive = true,
                    Fragments = outgoingFragments
                };

                return memoryParts;
            }
        }

        public void HandleAck(ArraySegment<byte> payload)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));
            // Read the raw fragment data
            ushort encodedFragment = (ushort)(payload.Array[payload.Offset + 2] | (ushort)(payload.Array[payload.Offset + 3] << 8));
            // The fragmentId is the last 15 least significant bits
            ushort fragment = (ushort)(encodedFragment & 32767);
            // IsFinal is the most significant bit
            bool isFinal = (ushort)((encodedFragment & 32768) >> 15) == 1;

            lock (_lock)
            {
                if (_sendSequencer[sequence].Alive && _sendSequencer[sequence].Fragments.VirtualCount > fragment && ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments.Pointers[fragment]).Alive)
                {
                    // Dealloc the memory held by the sequencer for the packet
                    ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments.Pointers[fragment]).DeAlloc(memoryManager);

                    // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                    // Get the roundtrp
                    ulong roundtrip = (ulong)Math.Round((NetTime.Now - ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments.Pointers[fragment]).FirstSent).TotalMilliseconds);

                    // Report to the connection
                    connection.AddRoundtripSample(roundtrip);

                    // Kill the fragment packet
                    _sendSequencer[sequence].Fragments.Pointers[fragment] = new PendingOutgoingFragment()
                    {
                        Alive = false,
                        Sequence = sequence
                    };

                    bool hasAllocatedAndAliveFragments = false;
                    for (int i = 0; i < _sendSequencer[sequence].Fragments.VirtualCount; i++)
                    {
                        if (_sendSequencer[sequence].Fragments.Pointers[i] != null && ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments.Pointers[i]).Alive)
                        {
                            hasAllocatedAndAliveFragments = true;
                            break;
                        }
                    }

                    if (!hasAllocatedAndAliveFragments)
                    {
                        // Dealloc the wrapper packet
                        _sendSequencer[sequence].DeAlloc(memoryManager);

                        // Kill the wrapper packet
                        _sendSequencer[sequence] = new PendingOutgoingPacket()
                        {
                            Alive = false
                        };

                        if (sequence == (ushort)(_outgoingLowestAckedSequence + 1))
                        {
                            // This was the next one.
                            _outgoingLowestAckedSequence++;
                        }
                    }
                }

                // Loop from the lowest ack we got
                for (ushort i = _outgoingLowestAckedSequence; !_sendSequencer[i].Alive && !_sendSequencer[i].AllFragmentsAlive && SequencingUtils.Distance(i, _lastOutgoingSequence, sizeof(ushort)) <= 0; i++)
                {
                    _outgoingLowestAckedSequence = i;
                }
            }
        }

        private void SendAck(ushort sequence, ushort fragment, bool isFinal)
        {
            // Alloc ack memory
            HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

            // Write header
            ackMemory.Buffer[0] = HeaderPacker.Pack(MessageType.Ack);
            ackMemory.Buffer[1] = (byte)channelId;

            // Write sequence
            ackMemory.Buffer[2] = (byte)sequence;
            ackMemory.Buffer[3] = (byte)(sequence >> 8);

            // Write fragment
            ackMemory.Buffer[4] = (byte)(fragment & 32767);
            ackMemory.Buffer[5] = (byte)(((byte)((fragment & 32767) >> 8)) | (byte)(isFinal ? 128 : 0));

            // Send ack
            connection.Send(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
        }

        private void SendAckEncoded(ushort sequence, ushort encodedFragment)
        {
            // Alloc ack memory
            HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

            // Write header
            ackMemory.Buffer[0] = HeaderPacker.Pack(MessageType.Ack);
            ackMemory.Buffer[1] = (byte)channelId;

            // Write sequence
            ackMemory.Buffer[2] = (byte)sequence;
            ackMemory.Buffer[3] = (byte)(sequence >> 8);

            // Write fragment
            ackMemory.Buffer[4] = (byte)encodedFragment;
            ackMemory.Buffer[5] = (byte)(encodedFragment >> 8);

            // Send ack
            connection.Send(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
        }

        public void InternalUpdate(out bool timeout)
        {
            lock (_lock)
            {
                for (ushort i = (ushort)(_outgoingLowestAckedSequence + 1); SequencingUtils.Distance(i, _lastOutgoingSequence, sizeof(ushort)) < 0; i++)
                {
                    if (_sendSequencer[i].Alive)
                    {
                        for (int j = 0; j < _sendSequencer[i].Fragments.VirtualCount; j++)
                        {
                            if (_sendSequencer[i].Fragments.Pointers[j] != null && ((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Alive)
                            {
                                if ((NetTime.Now - ((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).LastSent).TotalMilliseconds > connection.SmoothRoundtrip * config.ReliabilityResendRoundtripMultiplier && (NetTime.Now - ((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).LastSent).TotalMilliseconds > config.ReliabilityMinPacketResendDelay)
                                {
                                    if (((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Attempts > config.ReliabilityMaxResendAttempts)
                                    {
                                        // If they don't ack the message, disconnect them
                                        timeout = true;
                                        return;
                                    }

                                    _sendSequencer[i].Fragments.Pointers[j] = new PendingOutgoingFragment()
                                    {
                                        Alive = true,
                                        Attempts = (ushort)(((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Attempts + 1),
                                        LastSent = NetTime.Now,
                                        FirstSent = ((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).FirstSent,
                                        Memory = ((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Memory,
                                        Sequence = i
                                    };

                                    connection.Send(new ArraySegment<byte>(((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Memory.Buffer, (int)((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Memory.VirtualOffset, (int)((PendingOutgoingFragment)_sendSequencer[i].Fragments.Pointers[j]).Memory.VirtualCount), false);
                                }
                            }
                        }
                    }
                }
            }

            timeout = false;
        }

        public void Release()
        {
            lock (_lock)
            {
                // Clear all incoming states
                _incomingAckedSequences.Clear();
                _receiveSequencer.Release();
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
