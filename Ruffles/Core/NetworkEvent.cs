using System;
using Ruffles.Connections;
using Ruffles.Exceptions;
using Ruffles.Memory;

namespace Ruffles.Core
{
    public struct NetworkEvent
    {
        public NetworkEventType Type { get; internal set; }
        public Listener Listener { get; internal set; }
        public Connection Connection { get; internal set; }
        public ArraySegment<byte> Data { get; internal set; }

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
