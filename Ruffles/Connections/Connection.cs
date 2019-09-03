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
        public override double Roundtrip { get; internal set; } = 10;
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

        internal override void SendRaw(ArraySegment<byte> payload, bool noMerge)
        {
            Socket.SendRaw(this, payload, noMerge);
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
