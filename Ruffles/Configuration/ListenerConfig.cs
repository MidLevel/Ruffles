using System.Net;
using Ruffles.Channeling;
using Ruffles.Simulation;

namespace Ruffles.Configuration
{
    public class ListenerConfig
    {
        // Connection
        public IPAddress IPv4ListenAddress = IPAddress.Any;
        public IPAddress IPv6ListenAddress = IPAddress.IPv6Any;
        public ushort DualListenPort = 5674;
        public bool UseIPv6Dual = true;

        // Performance
        public ushort MaxSocketBlockMilliseconds = 5;
        public ulong MinConnectionPollDelay = 50;
        public bool EnableThreadSafety = false;
        public bool ReuseConnections = true;

        // Bandwidth
        public ushort MaxMergeMessageSize = 256;
        public ulong MinMergeDelay = 250;

        // Memory
        public ushort MaxBufferSize = ushort.MaxValue;
        public ushort MaxConnections = ushort.MaxValue;

        // Timeouts
        public ulong HandshakeTimeout = 10000;
        public ulong ConnectionTimeout = 10000;
        public ulong MinHeartbeatDelay = 2000;

        // Handshake resends
        public ulong HandshakeMinResendDelay = 200;
        public byte MaxHandshakeResends = 20;

        // Connection request resends
        public ulong ConnectionRequestMinResendDelay = 50;
        public byte MaxConnectionRequestResends = 5;

        // Security
        public byte ChallengeDifficulty = 10;

        // Denial Of Service
        public ushort MaxPendingConnections = ushort.MaxValue;
        public ushort AmplificationPreventionHandshakePadding = 256;

        // Channels
        public ChannelType[] ChannelTypes = new ChannelType[0];

        // Channel performance
        public ushort ReliabilityWindowSize = 512;
        public ulong ReliabilityMaxResendAttempts = 30;
        public double ReliabilityResendRoundtripMultiplier = 1.2;

        // Simulation
        public bool UseSimulator = false;
        public SimulatorConfig SimulatorConfig = new SimulatorConfig()
        {
            DropPercentage = 0.2f,
            MaxLatency = 2000,
            MinLatency = 50
        };

        // Advanced protocol settings (usually these shoudl NOT be fucked with. Please understand their full meaning before changing)
        public bool EnableHeartbeats = true;
        public bool EnableTimeouts = true;
        public bool EnableChannelUpdates = true;
        public bool EnableConnectionRequestResends = true;
        // This one has to be the same on both ends.
        public bool EnablePacketMerging = true;
    }
}
