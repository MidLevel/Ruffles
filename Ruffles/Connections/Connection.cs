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
    public class Connection : ConnectionBase
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
        /// Gets the estimated roundtrip.
        /// </summary>
        /// <value>The estimated roundtrip.</value>
        public override double Roundtrip { get; internal set; } = 0;
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
            if (config.EnableHeartbeats)
            {
                HeartbeatChannel = new UnreliableSequencedChannel(0, this, config, memoryManager);
            }
        }

        internal override void SendRaw(ArraySegment<byte> payload, bool noMerge, ushort headerSize)
        {
            Socket.SendRaw(this, payload, noMerge, headerSize);
        }

        internal override void Disconnect(bool sendMessage)
        {
            Socket.DisconnectConnection(this, sendMessage, false);
        }

        internal override void AddRoundtripSample(ulong sample)
        {
            double rttDistance = sample - Roundtrip;
            Roundtrip += (rttDistance * 0.1d);
        }

        internal void Reset()
        {
            Dead = false;
            Recycled = false;
            State = ConnectionState.Disconnected;
            Socket = null;
            EndPoint = null;

            HailStatus = new MessageStatus();

            ConnectionChallenge = 0;
            ChallengeDifficulty = 0;
            ChallengeAnswer = 0;

            LastMessageOut = DateTime.MinValue;
            LastMessageIn = DateTime.MinValue;
            ConnectionStarted = DateTime.MinValue;

            Roundtrip = 0;

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

        /// <summary>
        /// Recycle this connection so that it can be reused by Ruffles.
        /// </summary>
        public void Recycle()
        {
            if (Dead && !Recycled)
            {
                Recycled = true;
            }
        }
    }
}
