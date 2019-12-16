using System;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Memory;

namespace Ruffles.Channeling
{
    internal interface IChannel
    {
        HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes);
        HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc);
        void HandleAck(ArraySegment<byte> payload);
        void Release();
        void Assign(byte channelId, Connection connection, SocketConfig config, MemoryManager memoryManager);
        void InternalUpdate();
    }
}
