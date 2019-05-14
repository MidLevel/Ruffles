using System;
using Ruffles.Channeling;
using Ruffles.Connections;
using Ruffles.Core;
using Ruffles.Memory;

namespace Ruffles.Messaging
{
    internal static class PacketHandler
    {
        internal static void HandleIncomingMessage(ArraySegment<byte> payload, Connection connection)
        {
            // This is where all data packets arrive after passing the connection handling.

            // TODO:
            // 1. Fragmentation
            // 2. Delay to pack messages

            byte channelId = payload.Array[payload.Offset];

            ArraySegment<byte>? incomingMessage = connection.Channels[channelId].HandleIncomingMessagePoll(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), out bool hasMore);

            if (incomingMessage != null)
            {
                // Alloc memory that can be borrowed to userspace
                HeapMemory memory = MemoryManager.Alloc(incomingMessage.Value.Count);

                // Copy payload to borrowed memory
                Buffer.BlockCopy(incomingMessage.Value.Array, incomingMessage.Value.Offset, memory.Buffer, 0, incomingMessage.Value.Count);

                // Send to userspace
                connection.Listener.EventQueueLock.EnterWriteLock();
                try
                {
                    connection.Listener.UserEventQueue.Enqueue(new NetworkEvent()
                    {
                        ConnectionId = connection.Id,
                        Listener = connection.Listener,
                        Type = NetworkEventType.Data,
                        AllowUserRecycle = true,
                        Data = new ArraySegment<byte>(memory.Buffer, memory.VirtualOffset, memory.VirtualCount),
                        InternalMemory = memory
                    });
                }
                finally
                {
                    connection.Listener.EventQueueLock.ExitWriteLock();
                }
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
                        connection.Listener.EventQueueLock.EnterWriteLock();
                        try
                        {
                            connection.Listener.UserEventQueue.Enqueue(new NetworkEvent()
                            {
                                ConnectionId = connection.Id,
                                Listener = connection.Listener,
                                Type = NetworkEventType.Data,
                                AllowUserRecycle = true,
                                Data = new ArraySegment<byte>(messageMemory.Buffer, messageMemory.VirtualOffset, messageMemory.VirtualCount),
                                InternalMemory = messageMemory
                            });
                        }
                        finally
                        {
                            connection.Listener.EventQueueLock.ExitWriteLock();
                        }
                    }
                }
                while (messageMemory != null);
            }
        }

        internal static void SendMessage(ArraySegment<byte> payload, Connection connection, byte channelId)
        {
            // TODO: Safety
            IChannel channel = connection.Channels[channelId];

            HeapMemory messageMemory = channel.CreateOutgoingMessage(payload, out bool dealloc);

            if (messageMemory != null)
            {
                connection.SendRaw(new ArraySegment<byte>(messageMemory.Buffer, messageMemory.VirtualOffset, messageMemory.VirtualCount));
            }

            if (dealloc)
            {
                // DeAlloc the memory again. This is done for unreliable channels that need the message after the initial send.
                MemoryManager.DeAlloc(messageMemory);
            }
        }
    }
}
