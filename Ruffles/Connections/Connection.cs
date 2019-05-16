using System;
using System.Net;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Configuration;
using Ruffles.Core;
using Ruffles.Messaging;

namespace Ruffles.Connections
{
    public class Connection
    {
        public ulong Id { get; internal set; }
        public bool Dead { get; internal set; }
        internal bool Recycled { get; set; }
        public ConnectionState State { get; internal set; }
        internal MessageStatus HailStatus;
        public EndPoint EndPoint { get; internal set; }
        public Listener Listener { get; internal set; }
        internal ulong ConnectionChallenge { get; set; }
        internal byte ChallengeDifficulty { get; set; }
        internal ulong ChallengeAnswer { get; set; }
        public DateTime LastMessageOut { get; internal set; }
        public DateTime LastMessageIn { get; internal set; }
        public DateTime ConnectionStarted { get; internal set; }
        public double Roundtrip { get; internal set; } = 10;
        internal readonly UnreliableSequencedChannel HeartbeatChannel;
        internal MessageMerger Merger;
        internal IChannel[] Channels;
        internal ChannelType[] ChannelTypes;

        // Handshake resend values
        internal byte HandshakeResendAttempts;
        internal DateTime HandshakeLastSendTime;


        internal Connection(ListenerConfig config)
        {
            if (config.EnableHeartbeats)
            {
                HeartbeatChannel = new UnreliableSequencedChannel(0, this);
            }
        }

        internal void SendRaw(ArraySegment<byte> payload, bool noMerge)
        {
            // TODO: Dead & state safety
            Listener.SendRaw(this, payload, noMerge);
        }

        internal void Disconnect(bool sendMessage)
        {
            // TODO: Dead & state safety
            Listener.DisconnectConnection(this, sendMessage, false);
        }

        internal void AddRoundtripSample(ulong sample)
        {
            // TODO: Dead & state safety

            // Old TCP:
            // Roundtrip = 0.0125 * Roundtrip + (1 - 0.0125) * sample;


            double rttDistance = sample - Roundtrip;
            Roundtrip += (rttDistance * 0.1d);
        }

        public void Recycle()
        {
            if (Dead && !Recycled)
            {
                Recycled = true;
            }
        }
    }
}
