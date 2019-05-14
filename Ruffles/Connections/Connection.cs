using System;
using System.Net;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Core;
using Ruffles.Messaging;

namespace Ruffles.Connections
{
    internal class Connection
    {
        public ushort Id;
        public bool Dead;
        public ConnectionState State;
        public MessageStatus HailStatus;
        public EndPoint EndPoint;
        public Listener Listener;
        public ulong ConnectionChallenge;
        public byte ChallengeDifficulty;
        public ulong ChallengeAnswer;
        public DateTime LastMessageOut;
        public DateTime LastMessageIn;
        public DateTime ConnectionStarted;
        public double Roundtrip = 10;
        public readonly UnreliableSequencedChannel HeartbeatChannel;

        public IChannel[] Channels;

        // Handshake resend values
        public byte HandshakeResendAttempts;
        public DateTime HandshakeLastSendTime;


        internal Connection()
        {
            HeartbeatChannel = new UnreliableSequencedChannel(0, this);
        }

        internal void SendRaw(ArraySegment<byte> payload)
        {
            // TODO: Dead & state safety
            Listener.SendRaw(this, payload);
        }

        internal void Disconnect(bool sendMessage)
        {
            // TODO: Dead & state safety
            Listener.DisconnectConnection(this, sendMessage);
        }

        internal void AddRoundtripSample(ulong sample)
        {
            // TODO: Dead & state safety
            Roundtrip = 0.0125 * Roundtrip + (1 - 0.0125) * sample;
        }
    }
}
