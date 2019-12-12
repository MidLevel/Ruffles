using System.Net;
using Ruffles.Connections;

namespace Ruffles.Core
{
    internal struct InternalEvent
    {
        internal InternalEventType Type;
        // Disconnect
        internal Connection Connection;
        internal bool SendMessage;
        // Connect
        internal EndPoint Endpoint;
        internal ulong PreConnectionChallengeTimestamp;
        internal ulong PreConnectionChallengeIV;
        internal ulong PreConnectionChallengeCounter;

        internal enum InternalEventType
        {
            Disconnect,
            Connect
        }
    }
}
