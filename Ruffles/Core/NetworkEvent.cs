using System;
using Ruffles.Exceptions;
using Ruffles.Memory;

namespace Ruffles.Core
{
    public struct NetworkEvent
    {
        public NetworkEventType Type;
        public Listener Listener;
        public ushort ConnectionId;
        public ArraySegment<byte> Data;
        internal HeapMemory InternalMemory;
        internal bool AllowUserRecycle;

        public void Recycle()
        {
            if (InternalMemory != null)
            {
                if (!AllowUserRecycle)
                {
                    throw new MemoryException("Cannot deallocate non recyclable memory");
                }

                MemoryManager.DeAlloc(InternalMemory);
            }
        }
    }
}
