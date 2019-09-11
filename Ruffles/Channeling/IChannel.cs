using System;
using Ruffles.Memory;

namespace Ruffles.Channeling
{
    internal interface IChannel
    {
        HeapMemory HandlePoll();
        ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes, out bool hasMore);
        HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc);
        void HandleAck(ArraySegment<byte> payload);
        void Reset();
        void InternalUpdate();
    }
}
