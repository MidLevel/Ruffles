using System;

namespace Ruffles.Connections
{
    public abstract class ConnectionBase
    {
        internal abstract void SendRaw(ArraySegment<byte> payload, bool noMerge);
        internal abstract void Disconnect(bool sendMessage);
        internal abstract void AddRoundtripSample(ulong sample);
        public abstract double Roundtrip { get; internal set; }
    }
}
