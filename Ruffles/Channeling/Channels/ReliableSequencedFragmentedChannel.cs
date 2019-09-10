using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Utils;

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
                        if (Fragments[i] != null && ((PendingOutgoingFragment)Fragments[i]).Alive && ((PendingOutgoingFragment)Fragments[i]).IsAlloced)
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }

            public object[] Fragments;
            public bool Alive;

            public bool AllFragmentsAlive
            {
                get
                {
                    if (Fragments == null)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.Length; i++)
                    {
                        if (!((PendingOutgoingFragment)Fragments[i]).Alive)
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
                    for (int i = 0; i < Fragments.Length; i++)
                    {
                        if (((PendingOutgoingFragment)Fragments[i]).Alive && ((PendingOutgoingFragment)Fragments[i]).IsAlloced)
                        {
                            ((PendingOutgoingFragment)Fragments[i]).DeAlloc(memoryManager);
                        }
                    }

                    // Dealloc the pointers
                    memoryManager.DeAlloc(Fragments);
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
                    if (Fragments.Array == null)
                    {
                        return false;
                    }

                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        if (Fragments.Array[Fragments.Offset + i] != null && !((HeapMemory)Fragments.Array[Fragments.Offset + i]).isDead)
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
                        if (Fragments.Array[Fragments.Offset + i] == null || ((HeapMemory)Fragments.Array[Fragments.Offset + i]).isDead)
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
                        byteSize += ((HeapMemory)Fragments.Array[Fragments.Offset + i]).VirtualCount;
                    }

                    return byteSize;
                }
            }

            public ushort Sequence;
            public ushort? Size;
            public ArraySegment<object> Fragments;
            public bool Alive;

            public void DeAlloc(MemoryManager memoryManager)
            {
                if (IsAlloced)
                {
                    for (int i = 0; i < Fragments.Count; i++)
                    {
                        if (Fragments.Array[Fragments.Offset + i] != null && !((HeapMemory)Fragments.Array[Fragments.Offset + i]).isDead)
                        {
                            memoryManager.DeAlloc((HeapMemory)Fragments.Array[Fragments.Offset + i]);
                        }
                    }

                    memoryManager.DeAlloc(Fragments.Array);
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
        private readonly SocketConfig config;
        private readonly MemoryManager memoryManager;

        // Lock for the channel, this allows sends and receives being done on different threads.
        private readonly object _lock = new object();

        internal ReliableSequencedFragmentedChannel(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager)
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
            lock (_lock)
            {
                // Get the next packet that is not yet given to the user.
                PendingIncomingPacket nextPacket = _receiveSequencer[_incomingLowestAckedSequence + 1];

                if (nextPacket.Alive && nextPacket.IsComplete)
                {
                    ++_incomingLowestAckedSequence;

                    // Get the total size of all fragments
                    uint totalSize = nextPacket.TotalByteSize;

                    // Alloc memory for that large segment
                    HeapMemory memory = memoryManager.AllocHeapMemory(totalSize);

                    // Keep track of where we are, fragments COULD have different sizes.
                    int bufferPosition = 0;

                    // Copy all the parts
                    for (int i = 0; i < nextPacket.Fragments.Count; i++)
                    {
                        // Copy fragment to final buffer
                        Buffer.BlockCopy(((HeapMemory)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i]).Buffer, (int)((HeapMemory)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i]).VirtualOffset, memory.Buffer, bufferPosition, (int)((HeapMemory)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i]).VirtualCount);

                        bufferPosition += (int)((HeapMemory)nextPacket.Fragments.Array[nextPacket.Fragments.Offset + i]).VirtualCount;
                    }

                    // Free the memory of all the individual fragments
                    nextPacket.DeAlloc(memoryManager);

                    // Kill
                    _receiveSequencer[_incomingLowestAckedSequence] = new PendingIncomingPacket()
                    {
                        Alive = false,
                        Sequence = 0,
                        Size = null,
                        Fragments = new ArraySegment<object>()
                    };


                    // HandlePoll gives the memory straight to the user, they are responsible for deallocing to prevent leaks
                    return memory;
                }

                return null;
            }
        }

        public ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes, out bool hasMore)
        {
            // Read the sequence number
            ushort sequence = (ushort)(payload.Array[payload.Offset] | (ushort)(payload.Array[payload.Offset + 1] << 8));
            // Read the raw fragment data
            ushort encodedFragment = (ushort)(payload.Array[payload.Offset + 2] | (ushort)(payload.Array[payload.Offset + 3] << 8));
            // The fragmentId is the last 15 least significant bits
            ushort fragment = (ushort)(encodedFragment & 32767);
            // IsFinal is the most significant bit
            bool isFinal = (ushort)((encodedFragment & 32768) >> 15) == 1;

            // Set the headerBytes
            headerBytes = 4;

            if (fragment > config.MaxFragments)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("FragmentId was too large. [FragmentId=" + fragment + "] [Config.MaxFragments=" + config.MaxFragments + "]. The fragment was silently dropped, expect a timeout.");
                hasMore = false;
                return null;
            }

            lock (_lock)
            {
                if (SequencingUtils.Distance(sequence, _incomingLowestAckedSequence, sizeof(ushort)) <= 0 ||
                    (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].IsComplete) ||
                    (_receiveSequencer[sequence].Alive && _receiveSequencer[sequence].Fragments.Array != null && _receiveSequencer[sequence].Fragments.Count > fragment && _receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment] != null && !((HeapMemory)_receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment]).isDead))
                {
                    // We have already acked this message. Ack again

                    connection.IncomingDuplicatePackets++;
                    connection.IncomingDuplicateUserBytes += (ulong)payload.Count - 2;
                    connection.IncomingDuplicateTotalBytes += (ulong)payload.Count + 2;

                    SendAckEncoded(sequence, encodedFragment);

                    hasMore = false;

                    return null;
                }
                else
                {
                    // This is a packet after the last. One that is not yet completed

                    PendingIncomingPacket unsafeIncoming = _receiveSequencer.GetUnsafe(sequence, out bool isSafe);

                    if (unsafeIncoming.Alive && !isSafe)
                    {
                        if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Incoming packet window is exhausted. Disconnecting");

                        connection.Disconnect(false);

                        hasMore = false;
                        return null;
                    }
                    else if (!_receiveSequencer[sequence].Alive)
                    {
                        // If this is the first fragment we ever get, index the data.
                        int fragmentPointerSize = isFinal ? fragment + 1 : config.FragmentArrayBaseSize;

                        object[] fragmentPointers = memoryManager.AllocPointers((uint)fragmentPointerSize);

                        _receiveSequencer[sequence] = new PendingIncomingPacket()
                        {
                            Alive = true,
                            Fragments = new ArraySegment<object>(fragmentPointers, 0, fragment + 1),
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
                            uint allocSize = Math.Max(config.FragmentArrayBaseSize, MemoryManager.CalculateMultiple((uint)fragment + 1, 64));

                            // Alloc new array
                            ArraySegment<object> newFragments = new ArraySegment<object>(new HeapMemory[allocSize], _receiveSequencer[sequence].Fragments.Offset, fragment + 1);

                            // Copy old values
                            Array.Copy(_receiveSequencer[sequence].Fragments.Array, newFragments.Array, _receiveSequencer[sequence].Fragments.Array.Length);

                            // Return the memory for the old
                            memoryManager.DeAlloc(_receiveSequencer[sequence].Fragments.Array);

                            // Update the index
                            _receiveSequencer[sequence] = new PendingIncomingPacket()
                            {
                                Fragments = newFragments,
                                Alive = _receiveSequencer[sequence].Alive,
                                Sequence = _receiveSequencer[sequence].Sequence,
                                Size = isFinal ? (ushort?)(fragment + 1) : _receiveSequencer[sequence].Size
                            };
                        }

                        // We might also have to expand the virtual count
                        if (_receiveSequencer[sequence].Fragments.Count <= fragment)
                        {
                            // Update the virtual elngth of the array
                            _receiveSequencer[sequence] = new PendingIncomingPacket()
                            {
                                Fragments = new ArraySegment<object>(_receiveSequencer[sequence].Fragments.Array, _receiveSequencer[sequence].Fragments.Offset, fragment + 1),
                                Alive = _receiveSequencer[sequence].Alive,
                                Sequence = _receiveSequencer[sequence].Sequence,
                                Size = isFinal ? (ushort?)(fragment + 1) : _receiveSequencer[sequence].Size
                            };
                        }
                    }

                    if (_receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment] == null || ((HeapMemory)_receiveSequencer[sequence].Fragments.Array[_receiveSequencer[sequence].Fragments.Offset + fragment]).isDead)
                    {
                        // Alloc some memory for the fragment
                        HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count - 4);

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
        }

        public object[] CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc)
        {
            // Calculate the amount of fragments required
            int fragmentsRequired = (payload.Count + (connection.MTU - 1)) / connection.MTU;

            if (fragmentsRequired > config.MaxFragments)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Tried to create message that was too large. [Size=" + payload.Count + "] [FragmentsRequired=" + fragmentsRequired + "] [Config.MaxFragments=" + config.MaxFragments + "]");
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
                headerSize = 6;

                // Alloc array
                object[] memoryParts = memoryManager.AllocPointers((uint)fragmentsRequired);

                int position = 0;

                for (ushort i = 0; i < fragmentsRequired; i++)
                {
                    // Calculate message size
                    int messageSize = Math.Min(connection.MTU, payload.Count - position);

                    // Allocate memory for each fragment
                    memoryParts[i] = memoryManager.AllocHeapMemory((uint)(messageSize + 6));

                    // Write headers
                    ((HeapMemory)memoryParts[i]).Buffer[0] = HeaderPacker.Pack((byte)MessageType.Data, false);
                    ((HeapMemory)memoryParts[i]).Buffer[1] = channelId;

                    // Write the sequence
                    ((HeapMemory)memoryParts[i]).Buffer[2] = (byte)_lastOutboundSequenceNumber;
                    ((HeapMemory)memoryParts[i]).Buffer[3] = (byte)(_lastOutboundSequenceNumber >> 8);

                    // Write the fragment
                    ((HeapMemory)memoryParts[i]).Buffer[4] = (byte)(i & 32767);
                    ((HeapMemory)memoryParts[i]).Buffer[5] = (byte)(((i & 32767) >> 8) | (byte)(i == fragmentsRequired - 1 ? 128 : 0));

                    // Write the payload
                    Buffer.BlockCopy(payload.Array, payload.Offset + position, ((HeapMemory)memoryParts[i]).Buffer, 6, messageSize);

                    // Increase the position
                    position += messageSize;
                }

                // Tell the caller NOT to dealloc the memory, the channel needs it for resend purposes.
                dealloc = false;

                // Alloc outgoing fragment structs
                object[] outgoingFragments = memoryManager.AllocPointers((uint)fragmentsRequired);

                for (int i = 0; i < fragmentsRequired; i++)
                {
                    // Add the memory to the outgoing sequencer array
                    outgoingFragments[i] = new PendingOutgoingFragment()
                    {
                        Alive = true,
                        Attempts = 1,
                        LastSent = DateTime.Now,
                        FirstSent = DateTime.Now,
                        Sequence = _lastOutboundSequenceNumber,
                        Memory = ((HeapMemory)memoryParts[i])
                    };
                }

                // Add the memory to the outgoing sequencer
                _sendSequencer[_lastOutboundSequenceNumber] = new PendingOutgoingPacket()
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
                if (_sendSequencer[sequence].Alive && _sendSequencer[sequence].Fragments.Length > fragment && ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments[fragment]).Alive)
                {
                    // Add statistics
                    connection.OutgoingConfirmedPackets++;

                    // Dealloc the memory held by the sequencer for the packet
                    ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments[fragment]).DeAlloc(memoryManager);

                    // TODO: Remove roundtripping from channeled packets and make specific ping-pong packets

                    // Get the roundtrp
                    ulong roundtrip = (ulong)Math.Round((DateTime.Now - ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments[fragment]).FirstSent).TotalMilliseconds);

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
                        if (_sendSequencer[sequence].Fragments[i] != null && ((PendingOutgoingFragment)_sendSequencer[sequence].Fragments[i]).Alive)
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
                    }
                }

                for (ushort i = sequence; _sendSequencer[i].Alive && _sendSequencer[i].AllFragmentsAlive; i++)
                {
                    _incomingLowestAckedSequence = i;
                }
            }
        }

        public void Reset()
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

        private void SendAck(ushort sequence, ushort fragment, bool isFinal)
        {
            // Alloc ack memory
            HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

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
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false, 6);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
        }

        private void SendAckEncoded(ushort sequence, ushort encodedFragment)
        {
            // Alloc ack memory
            HeapMemory ackMemory = memoryManager.AllocHeapMemory(4);

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
            connection.SendRaw(new ArraySegment<byte>(ackMemory.Buffer, 0, 6), false, 6);

            // Return memory
            memoryManager.DeAlloc(ackMemory);
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
                        for (int j = 0; j < _sendSequencer[i].Fragments.Length; j++)
                        {
                            if (_sendSequencer[i].Fragments[j] != null && ((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Alive)
                            {
                                if (((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Attempts > config.ReliabilityMaxResendAttempts)
                                {
                                    // If they don't ack the message, disconnect them
                                    connection.Disconnect(false);
                                    return;
                                }
                                else if ((DateTime.Now - ((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).LastSent).TotalMilliseconds > connection.Roundtrip * config.ReliabilityResendRoundtripMultiplier)
                                {
                                    _sendSequencer[i].Fragments[j] = new PendingOutgoingFragment()
                                    {
                                        Alive = true,
                                        Attempts = (ushort)(((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Attempts + 1),
                                        LastSent = DateTime.Now,
                                        FirstSent = ((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).FirstSent,
                                        Memory = ((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Memory,
                                        Sequence = i
                                    };

                                    connection.SendRaw(new ArraySegment<byte>(((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Memory.Buffer, (int)((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Memory.VirtualOffset, (int)((PendingOutgoingFragment)_sendSequencer[i].Fragments[j]).Memory.VirtualCount), false, 6);

                                    connection.OutgoingResentPackets++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
 