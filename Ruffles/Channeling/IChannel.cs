using System;
using Ruffles.Memory;

namespace Ruffles.Channeling
{
    internal interface IChannel
    {
        HeapPointers HandleIncomingMessagePoll(ArraySegment<byte> payload, out byte headerBytes);
        HeapPointers CreateOutgoingMessage(ArraySegment<byte> payload, out byte headerSize, out bool dealloc);
        void HandleAck(ArraySegment<byte> payload);
        void Reset();
        void InternalUpdate();
    }
}
