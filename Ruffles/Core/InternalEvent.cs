using System.Net;
using Ruffles.Connections;
using Ruffles.Memory;

namespace Ruffles.Core
{
    internal struct InternalEvent
    {
        // All
        internal InternalEventType Type;
        // Disconnect and Send
        internal Connection Connection;
        // Disconnect
        internal bool SendMessage;
        // Connect
        internal EndPoint Endpoint;
        internal ulong PreConnectionChallengeTimestamp;
        internal ulong PreConnectionChallengeIV;
        internal ulong PreConnectionChallengeCounter;
        // Send
        internal HeapMemory Data;
        internal byte ChannelId;
        internal bool NoDelay;

        internal enum InternalEventType
        {
            Disconnect,
            Connect,
            Send
        }
    }
}
