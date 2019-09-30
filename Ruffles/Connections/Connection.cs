#define ALLOW_CONNECTION_STUB

using System;
using System.Net;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Configuration;
using Ruffles.Core;
using Ruffles.Memory;
using Ruffles.Messaging;

namespace Ruffles.Connections
{
    /// <summary>
    /// A connection between two RuffleSockets.
    /// </summary>
    public class Connection
    {
        /// <summary>
        /// Gets the id of the connection. This is reused for connections by the RuffleSocket.
        /// </summary>
        /// <value>The connectionId.</value>
        public ulong Id { get; internal set; }
        /// <summary>
        /// Gets a value indicating whether this <see cref="T:Ruffles.Connections.Connection"/> is dead.
        /// </summary>
        /// <value><c>true</c> if dead; otherwise, <c>false</c>.</value>
        public bool Dead { get; internal set; }
        internal bool Recycled { get; set; }
        /// <summary>
        /// Gets the connection state.
        /// </summary>
        /// <value>The connection state.</value>
        public ConnectionState State { get; internal set; }
        internal MessageStatus HailStatus;

        // MTU
        internal MessageStatus MTUStatus;

        /// <summary>
        /// Gets the current connection end point.
        /// </summary>
        /// <value>The connection end point.</value>
        public EndPoint EndPoint { get; internal set; }
        /// <summary>
        /// Gets the RuffleSocket the connection belongs to.
        /// </summary>
        /// <value>The RuffleSocket the connection belongs to.</value>
        public RuffleSocket Socket { get; internal set; }
        internal ulong ConnectionChallenge { get; set; }
        internal byte ChallengeDifficulty { get; set; }
        internal ulong ChallengeAnswer { get; set; }
        /// <summary>
        /// Gets the time of the last outbound message.
        /// </summary>
        /// <value>The time of the last outbound message.</value>
        public DateTime LastMessageOut { get; internal set; }
        /// <summary>
        /// Gets the time of the last incoming message.
        /// </summary>
        /// <value>The time of the last incoming message.</value>
        public DateTime LastMessageIn { get; internal set; }
        /// <summary>
        /// Gets the time the connection was started.
        /// </summary>
        /// <value>The time the connection started.</value>
        public DateTime ConnectionStarted { get; internal set; }
        /// <summary>
        /// Gets the estimated smoothed roundtrip.
        /// </summary>
        /// <value>The estimated smoothed roundtrip.</value>
        public uint SmoothRoundtrip { get; internal set; }
        /// <summary>
        /// Gets the mean roundtrip.
        /// </summary>
        /// <value>The roundtrip.</value>
        public uint Roundtrip { get; internal set; }
        /// <summary>
        /// Gets the roundtrip varience.
        /// </summary>
        /// <value>The roundtrip varience.</value>
        public uint RoundtripVarience { get; internal set; }
        /// <summary>
        /// Gets the lowest roundtrip time recorded.
        /// </summary>
        /// <value>The lowest roundtrip.</value>
        public uint LowestRoundtrip { get; internal set; }
        /// <summary>
        /// Gets the highest roundtrip varience recorded.
        /// </summary>
        /// <value>The highest roundtrip varience.</value>
        public uint HighestRoundtripVarience { get; internal set; }

        /// <summary>
        /// Gets the total amount of outgoing packets. This counts merged packets as individual packets, rather than one merge packet.
        /// </summary>
        /// <value>The total amount of packets.</value>
        public ulong OutgoingPackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of outgoing packets. This counts merged packets as one packet. This is the real amount of UDP packets sent over the wire.
        /// </summary>
        /// <value>The total amount of packets.</value>
        public ulong OutgoingWirePackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of bytes the user has requested to send. This is only user payloads and does not include headers or protocol packets.
        /// </summary>
        /// <value>The total amount of user bytes.</value>
        public ulong OutgoingUserBytes { get; internal set; }
        /// <summary>
        /// Gets the total amount of outgoing bytes. This includes headers. Its the total amount of UDP bytes.
        /// </summary>
        /// <value>The total amount of bytes.</value>
        public ulong OutgoingTotalBytes { get; internal set; }

        /// <summary>
        /// Gets the total amount of packets that was resent due to a missing packet ack. 
        /// This does not neccecarly mean the amount of packets that was dropped, since acks can be delayed.
        /// </summary>
        /// <value>The total amount of resent packets.</value>
        public ulong OutgoingResentPackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of outgoing packets that was acked at least once.
        /// </summary>
        /// <value>The total amount of outgoing packets that was acked.</value>
        public ulong OutgoingConfirmedPackets { get; internal set; }

        /// <summary>
        /// Gets the total amount of incoming packets. This counts merged packets as individual packets, rather than one merge packet.
        /// </summary>
        /// <value>The total amount of packets.</value>
        public ulong IncomingPackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of incoming packets. This counts merged packets as one packet. This is the real amount of UDP packets sent over the wire.
        /// </summary>
        /// <value>The total amount of packets.</value>
        public ulong IncomingWirePackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of bytes that are delivered to the user. This is only user payloads and does not include headers or protocol packets.
        /// </summary>
        /// <value>The total amount of user bytes.</value>
        public ulong IncomingUserBytes { get; internal set; }
        /// <summary>
        /// Gets the total amount of incoming bytes. This includes headers. Its the total amount of UDP bytes.
        /// </summary>
        /// <value>The total amount of bytes.</value>
        public ulong IncomingTotalBytes { get; internal set; }

        /// <summary>
        /// Gets the total amount of duplicate packets. This is packets that have already been acked once.
        /// </summary>
        /// <value>The total amount of packets.</value>
        public ulong IncomingDuplicatePackets { get; internal set; }
        /// <summary>
        /// Gets the total amount of duplicate bytes. This is is only bytes in packets that have already been acked once. This includes headers. Its the total amount of UDP bytes.
        /// </summary>
        /// <value>The total amount of bytes.</value>
        public ulong IncomingDuplicateTotalBytes { get; set; }
        /// <summary>
        /// Gets the total amount of duplicate user bytes. This is only user payloads and does not include headers or protocol packets.
        /// </summary>
        /// <value>The total amount of bytes.</value>
        public ulong IncomingDuplicateUserBytes { get; set; }

        /// <summary>
        /// Gets the maximum amount of bytes that can be sent in a single message.
        /// </summary>
        /// <value>The maximum transmission unit.</value>
        public ushort MTU
        {
            get
            {
                return _mtu;
            }
            internal set
            {
                if (_mtu != value)
                {
                    _mtu = value;

                    if (OnMTUChanged != null)
                    {
                        OnMTUChanged(_mtu);
                    }
                }
            }
        }
        // Backing field for MTU property
        private ushort _mtu;
        /// <summary>
        /// Called when the MTU changes.
        /// </summary>
        public event MTUChangedDelegate OnMTUChanged;
        /// <summary>
        /// Delegate representing a MTU change.
        /// </summary>
        public delegate void MTUChangedDelegate(ushort MTU);

        internal readonly UnreliableSequencedChannel HeartbeatChannel;
        internal MessageMerger Merger;
        internal IChannel[] Channels;
        internal ChannelType[] ChannelTypes;

        // Pre connection challenge values
        internal ulong PreConnectionChallengeTimestamp;
        internal ulong PreConnectionChallengeCounter;
        internal ulong PreConnectionChallengeIV;


        // Handshake resend values
        internal byte HandshakeResendAttempts;
        internal DateTime HandshakeLastSendTime;

        internal Connection(SocketConfig config, MemoryManager memoryManager)
        {
#if ALLOW_CONNECTION_STUB
            if (IsStub)
            {
                // NOOP
                return;
            }
#endif
            if (config.EnableHeartbeats)
            {
                HeartbeatChannel = new UnreliableSequencedChannel(0, this, config, memoryManager);
            }
        }

        internal void SendRaw(ArraySegment<byte> payload, bool noMerge, ushort headerSize)
        {
#if ALLOW_CONNECTION_STUB
            if (IsStub)
            {
                // NOOP
                return;
            }
#endif
            Socket.SendRaw(this, payload, noMerge, headerSize);
        }

        internal void Disconnect(bool sendMessage)
        {
#if ALLOW_CONNECTION_STUB
            if (IsStub)
            {
                // NOOP
                return;
            }
#endif
            Socket.DisconnectConnection(this, sendMessage, false);
        }

        internal void AddRoundtripSample(ulong sample)
        {
            if (sample == 0)
            {
                sample = 1;
            }

            if (SmoothRoundtrip == 0)
            {
                SmoothRoundtrip = (uint)((1 - 0.125) + 0.125 * sample);
            }
            else
            {
                SmoothRoundtrip = (uint)((1 - 0.125) * SmoothRoundtrip + 0.125 * sample);
            }

            RoundtripVarience -= (RoundtripVarience / 4);

            if (SmoothRoundtrip >= Roundtrip)
            {
                Roundtrip += (SmoothRoundtrip - Roundtrip) / 8;
                RoundtripVarience += (SmoothRoundtrip - Roundtrip) / 4;
            }
            else
            {
                Roundtrip -= (Roundtrip - SmoothRoundtrip) / 8;
                RoundtripVarience += (Roundtrip - SmoothRoundtrip) / 4;
            }

            if (Roundtrip < LowestRoundtrip)
            {
                LowestRoundtrip = Roundtrip;
            }

            if (RoundtripVarience > HighestRoundtripVarience)
            {
                HighestRoundtripVarience = RoundtripVarience;
            }
        }

        internal void Reset()
        {
            Dead = false;
            Recycled = false;
            State = ConnectionState.Disconnected;
            Socket = null;
            EndPoint = null;

            HailStatus = new MessageStatus();

            MTUStatus = new MessageStatus();

            ConnectionChallenge = 0;
            ChallengeDifficulty = 0;
            ChallengeAnswer = 0;

            LastMessageOut = DateTime.MinValue;
            LastMessageIn = DateTime.MinValue;
            ConnectionStarted = DateTime.MinValue;

            SmoothRoundtrip = 0;
            Roundtrip = 500;
            LowestRoundtrip = 500;
            RoundtripVarience = 0;
            HighestRoundtripVarience = 0;

            MTU = 0;

            HandshakeLastSendTime = DateTime.MinValue;
            HandshakeResendAttempts = 0;

            HeartbeatChannel.Reset();
            Merger.Clear();

            PreConnectionChallengeTimestamp = 0;
            PreConnectionChallengeCounter = 0;
            PreConnectionChallengeIV = 0;

            OutgoingPackets = 0;
            OutgoingWirePackets = 0;
            OutgoingUserBytes = 0;
            OutgoingTotalBytes = 0;

            OutgoingResentPackets = 0;
            OutgoingConfirmedPackets = 0;

            IncomingPackets = 0;
            IncomingWirePackets = 0;
            IncomingUserBytes = 0;
            IncomingTotalBytes = 0;

            IncomingDuplicatePackets = 0;
            IncomingDuplicateTotalBytes = 0;
            IncomingDuplicateUserBytes = 0;

        }

#if ALLOW_CONNECTION_STUB
        private bool IsStub { get; set; }

        // Used by Test project
        internal static Connection Stub(SocketConfig config, MemoryManager manager)
        {
            return new Connection(config, manager)
            {
                IsStub = true,
                MTU = config.MinimumMTU
            };
        }
#endif
    }
}
