using System;
using Ruffles.Channeling;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Core;
using Ruffles.Memory;
using Ruffles.Utils;

namespace Ruffles.Messaging
{
    internal static class PacketHandler
    {
        internal static void HandleIncomingMessage(ArraySegment<byte> payload, Connection connection, SocketConfig activeConfig, MemoryManager memoryManager)
        {
            // This is where all data packets arrive after passing the connection handling.

            byte channelId = payload.Array[payload.Offset];

            if (channelId < 0 || channelId >= connection.Channels.Length)
            {
                // ChannelId out of range
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Got message on channel out of range. [ChannelId=" + channelId + "]");
                return;
            }

            DirectOrAllocedMemory incomingMessage = connection.Channels[channelId].HandleIncomingMessagePoll(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), out byte headerBytes, out bool hasMore);

            connection.IncomingUserBytes += (ulong)payload.Count - headerBytes;

            HeapMemory memory = null;

            if (incomingMessage.AllocedMemory != null)
            {
                // Use the alloced memory
                memory = incomingMessage.AllocedMemory;
            }
            else if (incomingMessage.DirectMemory != null)
            {
                // Alloc memory that can be borrowed to userspace
                memory = memoryManager.AllocHeapMemory((uint)incomingMessage.DirectMemory.Value.Count);

                // Copy payload to borrowed memory
                Buffer.BlockCopy(incomingMessage.DirectMemory.Value.Array, incomingMessage.DirectMemory.Value.Offset, memory.Buffer, 0, incomingMessage.DirectMemory.Value.Count);
            }

            if (memory != null)
            {
                // Send to userspace
                connection.Socket.PublishEvent(new NetworkEvent()
                {
                    Connection = connection,
                    Socket = connection.Socket,
                    Type = NetworkEventType.Data,
                    AllowUserRecycle = true,
                    Data = new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount),
                    InternalMemory = memory,
                    SocketReceiveTime = DateTime.Now,
                    ChannelId = channelId,
                    MemoryManager = memoryManager
                });
            }

            if (hasMore)
            {
                HeapMemory messageMemory = null;

                do
                {
                    messageMemory = connection.Channels[channelId].HandlePoll();

                    if (messageMemory != null)
                    {
                        // Send to userspace
                        connection.Socket.PublishEvent(new NetworkEvent()
                        {
                            Connection = connection,
                            Socket = connection.Socket,
                            Type = NetworkEventType.Data,
                            AllowUserRecycle = true,
                            Data = new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset, (int)messageMemory.VirtualCount),
                            InternalMemory = messageMemory,
                            SocketReceiveTime = DateTime.Now,
                            ChannelId = channelId,
                            MemoryManager = memoryManager
                        });
                    }
                }
                while (messageMemory != null);
            }
        }

        internal static void SendMessage(ArraySegment<byte> payload, Connection connection, byte channelId, bool noMerge, MemoryManager memoryManager)
        {
            if (channelId < 0 || channelId >= connection.Channels.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(channelId), channelId, "ChannelId was out of range");
            }

            IChannel channel = connection.Channels[channelId];

            HeapPointers memoryPointers = channel.CreateOutgoingMessage(payload, out byte headerSize, out bool dealloc);

            if (memoryPointers != null)
            {
                for (int i = 0; i < memoryPointers.VirtualCount; i++)
                {
                    connection.SendRaw(new ArraySegment<byte>(((HeapMemory)memoryPointers.Pointers[i]).Buffer, (int)((HeapMemory)memoryPointers.Pointers[i]).VirtualOffset, (int)((HeapMemory)memoryPointers.Pointers[i]).VirtualCount), noMerge, headerSize);
                }

                if (dealloc)
                {
                    // DeAlloc the memory again. This is done for unreliable channels that dont need the message after the initial send.
                    for (int i = 0; i < memoryPointers.VirtualCount; i++)
                    {
                        memoryManager.DeAlloc(((HeapMemory)memoryPointers.Pointers[i]));
                    }
                }

                // Dealloc the array always.
                memoryManager.DeAlloc(memoryPointers);
            }
        }
    }
}
