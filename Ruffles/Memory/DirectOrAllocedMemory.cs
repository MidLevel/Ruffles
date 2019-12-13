using System;

namespace Ruffles.Memory
{
    internal struct DirectOrAllocedMemory
    {
        internal HeapMemory AllocedMemory;
        internal ArraySegment<byte>? DirectMemory;
    }
}
