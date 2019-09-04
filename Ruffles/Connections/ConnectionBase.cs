using System;

namespace Ruffles.Connections
{
    public abstract class ConnectionBase
    {
        internal abstract void SendRaw(ArraySegment<byte> payload, bool noMerge, ushort headerSize);
        internal abstract void Disconnect(bool sendMessage);
        internal abstract void AddRoundtripSample(ulong sample);
        public abstract double Roundtrip { get; internal set; }

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
    }
}
