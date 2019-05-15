using System.Net;
using Ruffles.Memory;

namespace Ruffles.Threading
{
    internal struct ThreadHopEvent
    {
        public ulong ConnectionId;
        public byte ChannelId;
        public ThreadHopType Type;
        public HeapMemory Memory;
        public EndPoint Endpoint;
        public bool NoDelay;
    }
}
