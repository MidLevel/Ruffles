﻿using System;
using Ruffles.Memory;

namespace Ruffles.Channeling
{
    internal interface IChannel
    {
        HeapMemory HandlePoll();
        ArraySegment<byte>? HandleIncomingMessagePoll(ArraySegment<byte> payload, out bool hasMore);
        HeapMemory[] CreateOutgoingMessage(ArraySegment<byte> payload, out bool dealloc);
        void HandleAck(ArraySegment<byte> payload);
        void Reset();
        void InternalUpdate();
    }
}
