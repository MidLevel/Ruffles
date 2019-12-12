﻿using System.Collections.Generic;
using System.Net;
using Ruffles.Channeling;
using Ruffles.Simulation;

namespace Ruffles.Configuration
{
    public class SocketConfig
    {
        // General
        /// <summary>
        /// Whether or not to allow polling of the socket. This will require all messages to be processed in a queue.
        /// </summary>
        public bool EnablePollEvents = true;
        /// <summary>
        /// Whether or not to raise a callback when a event occurs.
        /// </summary>
        public bool EnableCallbackEvents = true;
        /// <summary>
        /// The size of the global event queue. 
        /// If this gets full no more events can be processed and the application will freeze until it is polled.
        /// </summary>
        public ushort EventQueueSize = 1024 * 8;
        /// <summary>
        /// The size of the internal event queue.
        /// </summary>
        public ushort InternalEventQueueSize = 1024;
        /// <summary>
        /// The maximum amount of heap pointers that will be kept as strong references by the memory manager.
        /// </summary>
        public ushort MemoryManagerMaxHeapPointers = 1024;
        /// <summary>
        /// The maximum amount of heap memory that will be kept as strong references by the memory manager.
        /// </summary>
        public ushort MemoryManagerMaxHeapMemory = 1024;

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
        public ushort DualListenPort = 0;
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
        /// </summary>
        public ushort SocketPollTime = 50;
        /// <summary>
        /// Whether or not to reuse connections. Disabling this has a impact on memory and CPU.
        /// If this is enabled, all connections has to be manually recycled by the user after receiving the disconnect or timeout events.
        /// </summary>
        public bool ReuseConnections = true;

        // Bandwidth
        /// <summary>
        /// The maximum size of a merged packet. 
        /// Increasing this increases the memory usage for each connection.
        /// </summary>
        public ushort MaxMergeMessageSize = 1024;
        /// <summary>
        /// The maximum delay before merged packets are sent.
        /// </summary>
        public ulong MaxMergeDelay = 100;
        /// <summary>
        /// Whether or not to enable merged acks for non fragmented channels.
        /// </summary>
        public bool EnableMergedAcks = true;
        /// <summary>
        /// The amount of bytes to use for merged acks.
        /// </summary>
        public byte MergedAckBytes = 8;

        // Fragmentation
        /// <summary>
        /// The maximum MTU size that will be attempted using path MTU.
        /// </summary>
        public ushort MaximumMTU = 4096;
        /// <summary>
        /// The minimum MTU size. This is the default maximum packet size.
        /// </summary>
        public ushort MinimumMTU = 512;
        /// <summary>
        /// Whether or not to enable path MTU.
        /// </summary>
        public bool EnablePathMTU = true;
        /// <summary>
        /// The maximum amount of MTU requests to attempt.
        /// </summary>
        public byte MaxMTUAttempts = 8;
        /// <summary>
        /// The delay in milliseconds between MTU resend attempts.
        /// </summary>
        public ulong MTUAttemptDelay = 1000;
        /// <summary>
        /// The MTU growth factor.
        /// </summary>
        public double MTUGrowthFactor = 1.25;

        /// <summary>
        /// The maximum amount of fragments allowed to be used.
        /// </summary>
        public ushort MaxFragments = 512;

        // Memory
        /// <summary>
        /// The maxmimum packet size. Should be larger than the MTU.
        /// </summary>
        public ushort MaxBufferSize = 1024 * 5;
        /// <summary>
        /// The maximum amount of connections. Increasing this increases the memory impact.
        /// </summary>
        public ushort MaxConnections = ushort.MaxValue;

        // Timeouts
        /// <summary>
        /// The amount of milliseconds from the connection request that the connection has to solve the challenge and complete the connection handshake.
        /// Note that this timeout only starts counting after the connection request has been approved.
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
        /// <summary>
        /// The maximum percentage of reliable packets that are allowed to be dropped before a connection times out.
        /// </summary>
        public double MaxPacketLossPercentage = 0.8;
        /// <summary>
        /// The maximum roundtrip time before a connection times out.
        /// </summary>
        public uint MaxRoundtripTime = 1500;
        /// <summary>
        /// The grace period for a connection where PacketLoss and Roundtrip timeouts are not checked.
        /// </summary>
        public ulong ConnectionQualityGracePeriod = 5000;

        // Handshake resends
        /// <summary>
        /// The amount of milliseconds between resends during the handshake process.
        /// </summary>
        public ulong HandshakeResendDelay = 500;
        /// <summary>
        /// The maximum amount of packet resends to perform per stage of the handshake process.
        /// </summary>
        public byte MaxHandshakeResends = 20;

        // Connection request resends
        /// <summary>
        /// The delay between connection request resends in milliseconds.
        /// </summary>
        public ulong ConnectionRequestMinResendDelay = 500;
        /// <summary>
        /// The maximum amount of connection requests to be sent.
        /// </summary>
        public byte MaxConnectionRequestResends = 5;
        /// <summary>
        /// The amount of time in milliseconds before a pending connection times out.
        /// </summary>
        public ulong ConnectionRequestTimeout = 5000;

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
        public ushort AmplificationPreventionHandshakePadding = 512;

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
        /// The window size for last ack times.
        /// </summary>
        public ushort ReliableAckFlowWindowSize = 1024;
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
        /// <summary>
        /// Whether or not to enable internal IO event queueing.
        /// Disabling this will prevent ConnectLater and DisconnectLater from working.
        /// </summary>
        public bool EnableQueuedIOEvents = true;

        public List<string> GetInvalidConfiguration()
        {
            List<string> messages = new List<string>();

            if (MaxFragments > 32768)
            {
                messages.Add("MaxFragments cannot be greater than 2^15=32768");
            }

            if (MaxMergeMessageSize > MaximumMTU)
            {
                messages.Add("MaxMergeMessageSize cannot be greater than MaxMessageSize");
            }

            if (AmplificationPreventionHandshakePadding > MaximumMTU)
            {
                messages.Add("AmplificationPreventionHandshakePadding cannot be greater than MaxMessageSize");
            }

            return messages;
        }
    }
}
