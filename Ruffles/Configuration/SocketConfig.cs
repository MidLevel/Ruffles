using System.Collections.Generic;
using System.Net;
using Ruffles.Channeling;
using Ruffles.Simulation;

namespace Ruffles.Configuration
{
    public class SocketConfig
    {
        // Connection
        /// <summary>
        /// The IPv4 address the socket will listen on.
        /// </summary>
        public IPAddress IPv4ListenAddress = IPAddress.Any;
        /// <summary>
        /// The IPv6 address the socket will listen on if UseDualIPv6 is turned on.
        /// </summary>
        public IPAddress IPv6ListenAddress = IPAddress.IPv6Any;
        /// <summary>
        /// The port that will be used to listen on both IPv4 and IPv6 if UseDualMode is turned on.
        /// </summary>
        public ushort DualListenPort = 5674;
        /// <summary>
        /// Whether or not the socket will listen on IPv4 and IPv6 in dual mode on the same port.
        /// </summary>
        public bool UseIPv6Dual = true;
        /// <summary>
        /// Whether or not unconnected messages should be allowed.
        /// </summary>
        public bool AllowUnconnectedMessages = false;

        // Performance
        /// <summary>
        /// The max socket block time in milliseconds. This will affect how long the internal loop will block.
        /// The amount is double for dual mode sockets.
        /// </summary>
        public ushort MaxSocketBlockMilliseconds = 5;
        /// <summary>
        /// The minimum delay for running the internal resend and timeout logic.
        /// </summary>
        public ulong MinConnectionPollDelay = 50;
        /// <summary>
        /// Whether or not to reuse connections. Disabling this has a impact on memory and CPU.
        /// If this is enabled, all connections has to be manually recycled by the user after receiving the disconnect or timeout events.
        /// </summary>
        public bool ReuseConnections = true;
        /// <summary>
        /// Whether or not to pool pointer arrays. This is benefitial if your application is garbage critical.
        /// </summary>
        public bool PoolPointerArrays = true;

        // Bandwidth
        /// <summary>
        /// The maximum size of a merged packet. 
        /// Increasing this increases the memory usage for each connection.
        /// </summary>
        public ushort MaxMergeMessageSize = 1450;
        /// <summary>
        /// The maximum delay before merged packets are sent.
        /// </summary>
        public ulong MaxMergeDelay = 250;
        public bool EnableMergedAcks = true;
        public byte MergedAckBytes = 8;

        // Fragmentation
        /// <summary>
        /// The maximum user size of a single message.
        /// </summary>
        public ushort MaxMessageSize = 1450;
        /// <summary>
        /// The default size of fragment arrays.
        /// </summary>
        public ushort FragmentArrayBaseSize = 64;
        /// <summary>
        /// The maximum amount of fragments allowed to be used.
        /// </summary>
        public ushort MaxFragments = 512;

        // Fragmentation
        /// <summary>
        /// The maximum user size of a single message.
        /// </summary>
        public ushort MaxMessageSize = 1450;
        /// <summary>
        /// The default size of fragment arrays.
        /// </summary>
        public ushort FragmentArrayBaseSize = 64;
        /// <summary>
        /// The maximum amount of fragments allowed to be used.
        /// </summary>
        public ushort MaxFragments = 512;

        // Memory
        /// <summary>
        /// The maxmimum packet size. Should be larger than the MTU.
        /// </summary>
        public ushort MaxBufferSize = 1500;
        /// <summary>
        /// The maximum amount of connections. Increasing this increases the memory impact.
        /// </summary>
        public ushort MaxConnections = ushort.MaxValue;

        // Timeouts
        /// <summary>
        /// The amount of milliseconds from the connection request that the connection has to solve the challenge and complete the connection handshake.
        /// </summary>
        public ulong HandshakeTimeout = 30_000;
        /// <summary>
        /// The amount of milliseconds of packet silence before a already connected connection will be disconnected.
        /// </summary>
        public ulong ConnectionTimeout = 30_000;
        /// <summary>
        /// The amount milliseconds between heartbeat keep-alive packets are sent.
        /// </summary>
        public ulong HeartbeatDelay = 20_000;

        // Handshake resends
        /// <summary>
        /// The amount of milliseconds between resends during the handshake process.
        /// </summary>
        public ulong HandshakeResendDelay = 200;
        /// <summary>
        /// The maximum amount of packet resends to perform per stage of the handshake process.
        /// </summary>
        public byte MaxHandshakeResends = 20;

        // Connection request resends
        public ulong ConnectionRequestMinResendDelay = 50;
        public byte MaxConnectionRequestResends = 5;

        // Security
        /// <summary>
        /// The difficulty of the challenge in bits. Higher difficulties exponentially increase the solve time.
        /// </summary>
        public byte ChallengeDifficulty = 20;
        /// <summary>
        /// The amount of successfull initialization vectors to keep for initial connection requests.
        /// </summary>
        public uint ConnectionChallengeHistory = 2048;
        /// <summary>
        /// The connection request challenge time window in seconds.
        /// </summary>
        public ulong ConnectionChallengeTimeWindow = 60 * 5;
        /// <summary>
        /// Whether or not to enable time based connection challenge. 
        /// Enabling this will prevent slot filling attacks but requires the connector and connection receivers times to be synced with a diff of
        /// no more than ((RTT / 2) + ConnectionChallengeTimeWindow) in either direction.
        /// This is a perfectly reasonable expectation. The time is sent as UTC.
        /// </summary>
        public bool TimeBasedConnectionChallenge = true;

        // Denial Of Service
        /// <summary>
        /// The maximum connection slots that can be used for pending connections. 
        /// This is to limit slot filling attacks that has solved the connection request challenge.
        /// </summary>
        public ushort MaxPendingConnections = ushort.MaxValue;
        /// <summary>
        /// The amplification prevention padding of handshake requests. 
        /// All handshake packets sent by the connector will be of this size.
        /// </summary>
        public ushort AmplificationPreventionHandshakePadding = 1024;

        // Channels
        /// <summary>
        /// The channel types, the indexes of which becomes the channelId.
        /// </summary>
        public ChannelType[] ChannelTypes = new ChannelType[0];

        // Channel performance
        /// <summary>
        /// The window size for reliable packets, reliable acks and unrelaible acks.
        /// </summary>
        public ushort ReliabilityWindowSize = 512;
        /// <summary>
        /// The maximum amount of resends reliable channels will attempt per packet before timing the connection out.
        /// </summary>
        public ulong ReliabilityMaxResendAttempts = 30;
        /// <summary>
        /// The resend time multiplier. The resend delay for reliable packets is (RTT * ReliabilityResendRoundtripMultiplier).
        /// This is to account for flucuations in the network.
        /// </summary>
        public double ReliabilityResendRoundtripMultiplier = 1.2;

        // Simulation
        /// <summary>
        /// Whether or not to enable the network condition simulator.
        /// </summary>
        public bool UseSimulator = false;
        /// <summary>
        /// The configuration for the network simulator.
        /// </summary>
        public SimulatorConfig SimulatorConfig = new SimulatorConfig()
        {
            DropPercentage = 0.2f,
            MaxLatency = 2000,
            MinLatency = 50
        };

        // Advanced protocol settings (usually these should NOT be fucked with. Please understand their full meaning before changing)
        /// <summary>
        /// Whether or not heartbeats should be sent and processed. 
        /// Disabling this requires you to ensure the connection stays alive by sending constant packets yourself.
        /// </summary>
        public bool EnableHeartbeats = true;
        /// <summary>
        /// Whether or not timeouts should be enabled. 
        /// Disabling this means connection requests and connected connections will never time out. Not recommended.
        /// </summary>
        public bool EnableTimeouts = true;
        /// <summary>
        /// Whether or not to enable channel updates.
        /// Disabling this will prevent channels such as Reliable channels to resend packets.
        /// </summary>
        public bool EnableChannelUpdates = true;
        /// <summary>
        /// Whether or not packets should be resent during the connection handshake.
        /// Disabling this requires 0 packet loss during the handshake.
        /// </summary>
        public bool EnableConnectionRequestResends = true;
        /// <summary>
        /// Whether or not packet merging should be enabled.
        /// </summary>
        public bool EnablePacketMerging = true;

        public List<string> GetInvalidConfiguration()
        {
            List<string> messages = new List<string>();

            if (MaxFragments > 32768)
            {
                messages.Add("MaxFragments cannot be greater than 2^15=32768");
            }

            if (MaxMergeMessageSize > MaxMessageSize)
            {
                messages.Add("MaxMergeMessageSize cannot be greater than MaxMessageSize");
            }

            return messages;
        }
    }
}
