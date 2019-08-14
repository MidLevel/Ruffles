using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Core;
using Ruffles.Memory;
using Ruffles.Messaging;

namespace Ruffles.Channeling.Channels
{
    // TODO: Make single fragment messages faster
    // TODO: Remove array allocs
    internal class ReliableSequencedFragmentedChannel : IChannel
    {
        internal struct PendingOutgoingPacket : IMemoryReleasable
        {
            public bool IsAlloced
            {
                get
                {
                    if (Fragments == null)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.Length; i++)
                    {
                        if (Fragments[i].Alive && Fragments[i].IsAlloced)
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }

            public PendingOutgoingFragment[] Fragments;
            public bool Alive;

            public void DeAlloc()
            {
                if (IsAlloced)
                {
                    for (int i = 0; i < Fragments.Length; i++)
                    {
                        if (Fragments[i].Alive && Fragments[i].IsAlloced)
                        {
                            Fragments[i].DeAlloc();
                        }
                    }
                }
            }
        }


        internal struct PendingOutgoingFragment : IMemoryReleasable
        {
            public bool IsAlloced => Memory != null && !Memory.isDead;

            public ushort Sequence;
            public HeapMemory Memory;
            public DateTime LastSent;
            public DateTime FirstSent;
            public ushort Attempts;
            public bool Alive;

            public void DeAlloc()
            {
                if (IsAlloced)
                {
                    MemoryManager.DeAlloc(Memory);
                }
            }
        }

        internal struct PendingIncomingPacket : IMemoryReleasable
        {
            public bool IsAlloced
            {
                get
                {
                    if (Fragments.Array == null)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        if (Fragments.Array[Fragments.Offset + i] != null && !Fragments.Array[Fragments.Offset + i].isDead)
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }

            public bool IsComplete
            {
                get
                {
                    if (Fragments.Array == null || Size == null || Fragments.Count < Size)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        if (Fragments.Array[Fragments.Offset + i] == null || Fragments.Array[Fragments.Offset + i].isDead)
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


                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        byteSize += Fragments.Array[Fragments.Offset + i].VirtualCount;
                    }

                    return byteSize;
                }
            }

            public ushort Sequence;
            public ushort? Size;
            public ArraySegment<HeapMemory> Fragments;
            public bool Alive;

            public void DeAlloc()
            {
                if (IsAlloced)
                {
                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        if (Fragments.Array[Fragments.Offset + i] != null && !Fragments.Array[Fragments.Offset + i].isDead)
                        {
                            MemoryManager.DeAlloc(Fragments.Array[Fragments.Offset + i]);
                        }
                    }
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
        private readonly Connection connection;
        private readonly RuffleSocket socket;
        private readonly SocketConfig config;

        internal ReliableSequencedFragmentedChannel(byte channelId, Connection connection, RuffleSocket socket, SocketConfig config)
        {
            this.channelId = channelId;
            this.connection = connection;
            this.socket = socket;
            this.config = config;

            // Alloc the in flight windows for receive and send
            _receiveSequencer = new HeapableSlidingWindow<PendingIncomingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort));
            _sendSequencer = new HeapableSlidingWindow<PendingOutgoingPacket>(config.ReliabilityWindowSize, true, sizeof(ushort));
        }

        public HeapMemory HandlePoll()
        {
            // Get the next packet that is not yet given to the user.
            PendingIncomingPacket nextPacket = _receiveSequencer[_incomingLowestAckedSequence + 1];

            if (nextPacket.Alive && nextPacket.IsComplete)
            {
                ++_incomingLowestAckedSequence;

                // Get the total size of all fragments
                uint totalSize = nextPacket.TotalByteSize;

                // Alloc memory for that large segment
                HeapMemory memory = MemoryManager.Alloc(totalSize);

                // Keep track of where we are, fragments COULD have different sizes.
                int bufferPosition = 0;

                // Copy all the parts
                for (int i = 0; i < nextPacket.Fragments.Count; i++)
                {
                    // Copy fragment to final buffer
                    Buffer.BlockCopy(nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i].Buffer, (int)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i].VirtualOffset, memory.Buffer, bufferPosition, (int)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i].VirtualCount);

                    bufferPosition += (int)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i].VirtualCount;
                }

                // Free the memory of all the individual fragments
                nextPacket.DeAlloc();

                // Kill
                _receiveSequencer[_incomingLowestAckedSequence] = new PendingIncomingPacket()
                {
                    Alive = false,
                    Sequence = 0,
                    Size = null,
                    Fragments = new ArraySegment<HeapMemory>()
                };


                // HandlePoll gives the memory straight to the user, they are responsible for deallocing to prevent leaks
                return memory;
            }

            return null;
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out bool hasMore)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));
            // Read the raw fragment data
            ushort encodedFragment = (ushort)(payload.Array[payload.Offset + 2] | (ushort)(payload.Array[payload.Offset + 3] << 8));
            // The fragmentId is the last 15 least significant bits
            ushort fragment = (ushort)(encodedFragment & 32767);
            // IsFinal is the most significant bit
            bool isFinal = (ushort)((encodedFragment & 32768) >> 15) == 1;

            if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 || 
                (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].IsComplete) || 
                (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].Fragments.Array != null && _receiveSequencer[sequence].Fragments.Count > fragment && _receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment] != null && !_receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment].isDead))
            {
                // We have already acked this message. Ack again

                SendAckEncoded(sequence, encodedFragment);

                hasMore = false;
                return null;
            }
            else
            {
                // This is a packet after the last. One that is not yet completed

                // If this is the first fragment we ever get, index the data.
                if (!_receiveSequencer[sequence].Alive)
                {
                    _receiveSequencer[sequence] = new PendingIncomingPacket()
                    {
                        Alive = true,
                        // TODO: Remove hardcoded values
                        Fragments = new ArraySegment<HeapMemory>(new HeapMemory[isFinal ? fragment + 1 : 128], 0, fragment + 1),
                        Sequence = sequence,
                        Size = isFinal ? (ushort?)(fragment + 1) : null
                    };
                }
                else
                {
                    // If the first fragment we got was fragment 1 / 500. The fragments array will only be of size 128. We need to potentially resize it
                    if (_receiveSequencer[sequence].Fragments.Array.Length - _receiveSequencer[sequence].Fragments.Offset <= fragment)
                    {
                        // We need to expand the fragments array.

                        // Calculate the target size of the array
                        // TODO: Remove hardcoded values
                        uint allocSize = Math.Max(128, MemoryManager.CalculateMultiple((uint)fragment + 1, 64));

                        // Alloc new array
                        ArraySegment<HeapMemory> newFragments = new ArraySegment<HeapMemory>(new HeapMemory[allocSize], _receiveSequencer[sequence].Fragments.Offset, fragment + 1);

                        // Copy old values
                        Array.Copy(_receiveSequencer[sequence].Fragments.Array, newFragments.Array, _receiveSequencer[sequence].Fragments.Array.Length);

                        // Update the index
                        _receiveSequencer[sequence] = new PendingIncomingPacket()
                        {
                            Fragments = newFragments,
                            Alive = _receiveSequencer[sequence].Alive,
                            Sequence = _receiveSequencer[sequence].Sequence,
                            Size = _receiveSequencer[sequence].Size
                        };
                    }

                    // We might also have to expand the virtual count
                    if (_receiveSequencer[sequence].Fragments.Count <= fragment)
                    {
                        // Update the virtual elngth of the array
                        _receiveSequencer[sequence] = new PendingIncomingPacket()
                        {
                            Fragments = new ArraySegment<HeapMemory>(_receiveSequencer[sequence].Fragments.Array, _receiveSequencer[sequence].Fragments.Offset, fragment + 1),
                            Alive = _receiveSequencer[sequence].Alive,
                            Sequence = _receiveSequencer[sequence].Sequence,
                            Size = _receiveSequencer[sequence].Size
                        };
                    }
                }


                {
                    // Alloc some memory for the fragment
                    HeapMemory memory = MemoryManager.Alloc((uint)payload.Count - 4);

                    // Copy the payload
                    Buffer.BlockCopy(payload.Array, payload.Offset + 4, memory.Buffer, 0, payload.Count - 4);

                    // Add fragment to index
                    _receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment] = memory;
                }

                // Send ack
                SendAckEncoded(sequence, encodedFragment);

                // Sequenced never returns the original memory. Thus we need to return null and tell the caller to Poll instead.
                hasMore = _receiveSequencer[_incomingLowestAckedSequence + 1].Alive && _receiveSequencer[_incomingLowestAckedSequence + 1].IsComplete;

                return null;
            }
        }

        public HeapMemory[] CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc)
        {
            // Increment the sequence number
            _lastOutboundSequenceNumber++;

            // Calculate the amount of fragments required
            int fragmentsRequired = (payload.Count + (config.MaxMessageSize - 1)) / config.MaxMessageSize;

            // Alloc array
            HeapMemory[] fragments = new HeapMemory[fragmentsRequired];

            int position = 0;

            for (ushort i = 0; i < fragments.Length; i++)
            {
                // Calculate message size
                int messageSize = Math.Min(config.MaxMessageSize, payload.Count - position);

                // Allocate memory for each fragment
                fragments[i] = MemoryManager.Alloc(((uint)Math.Min(config.MaxMessageSize, payload.Count - position)) + 6);

                // Write headers
                fragments[i].Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
                fragments[i].Buffer[1] = channelId;

                // Write the sequence
                fragments[i].Buffer[2] = (byte)_lastOutboundSequenceNumber;
                fragments[i].Buffer[3] = (byte)(_lastOutboundSequenceNumber >> 8);

                // Write the fragment
                fragments[i].Buffer[4] = (byte)(i & 32767);
                fragments[i].Buffer[5] = (byte)(((i & 32767) >> 8) | (byte)(i == fragments.Length - 1 ? 128 : 0));

                // Write the payload
                Buffer.BlockCopy(payload.Array, payload.Offset + position, fragments[i].Buffer, (int)fragments[i].VirtualOffset, messageSize);

                // Increase the position
                position += messageSize;
            }

            // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
            dealloc = false;

            // Alloc outgoing fragment structs
            PendingOutgoingFragment[] outgoingFragments = new PendingOutgoingFragment[fragments.Length];

            for (int i = 0; i < outgoingFragments.Length; i++)
            {
                // Add the memory to the outgoing sequencer
                outgoingFragments[i] = new PendingOutgoingFragment()
                {
                    Alive = true,
                    Attempts = 1,
                    LastSent = DateTime.Now,
                    FirstSent = DateTime.Now,
                    Sequence = _lastOutboundSequenceNumber,
                    Memory = fragments[i]
                };
            }

            return fragments;
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

            if (_sendSequencer[sequence].Alive && _sendSequencer[sequence].Fragments.Length > fragment && _sendSequencer[sequence].Fragments[fragment].Alive)
            {
                // Dealloc the memory held by the sequencer for the packet
                _sendSequencer[sequence].Fragments[fragment].DeAlloc();

                // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                // Get the roundtrp
                ulong roundtrip = (ulong)Math.Round((DateTime.Now - _sendSequencer[sequence].Fragments[fragment].FirstSent).TotalMilliseconds);

                // Report to the connection
                connection.AddRoundtripSample(roundtrip);

                // Kill the fragment packet
                _sendSequencer[sequence].Fragments[fragment] = new PendingOutgoingFragment()
                {
                    Alive = false,
                    Sequence = sequence
                };

                bool hasAllocatedAndAliveFragments = false;
                for (int i = 0; i < _sendSequencer[sequence].Fragments.Length; i++)
                {
                    if (_sendSequencer[sequence].Fragments[i].Alive)
                    {
                        hasAllocatedAndAliveFragments = true;
                        break;
                    }
                }

                if (!hasAllocatedAndAliveFragments)
                {
                    // Dealloc the wrapper packet
                    _sendSequencer[sequence].DeAlloc();

                    // Kill the wrapper packet
                    _sendSequencer[sequence] = new PendingOutgoingPacket()
                    {
                        Alive = false
                    };
                }
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

        private void SendAck(ushort sequence, ushort fragment, bool isFinal)
        {
            // Alloc ack memory
            HeapMemory ackMemory = MemoryManager.Alloc(4);

            // Write header
            ackMemory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Ack, false);
            ackMemory.Buffer[1] = (byte)channelId;

            // Write sequence
            ackMemory.Buffer[2] = (byte)sequence;
            ackMemory.Buffer[3] = (byte)(sequence >> 8);

            // Write fragment
            ackMemory.Buffer[4] = (byte)(fragment & 32767);
            ackMemory.Buffer[5] = (byte)(((byte)((fragment & 32767) >> 8)) | (byte)(isFinal ? 128 : 0));

            // Send ack
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false);

            // Return memory
            MemoryManager.DeAlloc(ackMemory);
        }

        private void SendAckEncoded(ushort sequence, ushort encodedFragment)
        {
            // Alloc ack memory
            HeapMemory ackMemory = MemoryManager.Alloc(4);

            // Write header
            ackMemory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Ack, false);
            ackMemory.Buffer[1] = (byte)channelId;

            // Write sequence
            ackMemory.Buffer[2] = (byte)sequence;
            ackMemory.Buffer[3] = (byte)(sequence >> 8);

            // Write fragment
            ackMemory.Buffer[4] = (byte)encodedFragment;
            ackMemory.Buffer[5] = (byte)(encodedFragment >> 8);

            // Send ack
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false);

            // Return memory
            MemoryManager.DeAlloc(ackMemory);
        }

        public void InternalUpdate()
        {
            long distance = SequencingUtils.Distance(_lastOutboundSequenceNumber, _incomingLowestAckedSequence, sizeof(ushort));

            for (ushort i = _incomingLowestAckedSequence; i < _incomingLowestAckedSequence + distance; i++)
            {
                if (_sendSequencer[i].Alive)
                {
                    for (int j = 0; j < _sendSequencer[i].Fragments.Length; j++)
                    {
                        if (_sendSequencer[i].Fragments[j].Attempts > config.ReliabilityMaxResendAttempts)
                        {
                            // If they don't ack the message, disconnect them
                            connection.Disconnect(false);
                        }
                        else if ((DateTime.Now - _sendSequencer[i].Fragments[j].LastSent).TotalMilliseconds > connection.Roundtrip * config.ReliabilityResendRoundtripMultiplier)
                        {
                            _sendSequencer[i].Fragments[j] = new PendingOutgoingFragment()
                            {
                                Alive = true,
                                Attempts = (ushort)(_sendSequencer[i].Fragments[j].Attempts + 1),
                                LastSent = DateTime.Now,
                                FirstSent = _sendSequencer[i].Fragments[j].FirstSent,
                                Memory = _sendSequencer[i].Fragments[j].Memory,
                                Sequence = i
                            };

                            connection.SendRaw(new ArraySegment<byte>(_sendSequencer[i].Fragments[j].Memory.Buffer, (int)_sendSequencer[i].Fragments[j].Memory.VirtualOffset, (int)_sendSequencer[i].Fragments[j].Memory.VirtualCount), false);
                        }
                    }
                }
            }
        }
    }
}
 