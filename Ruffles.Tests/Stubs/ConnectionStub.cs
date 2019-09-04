using System;
using Ruffles.Connections;

namespace Ruffles.Tests.Stubs
{
    public class ConnectionStub : ConnectionBase
    {
        public override double Roundtrip { get; internal set; } = 10;

        public event Action<ArraySegment<byte>> OnSendData;
        public event Action OnDisconnect;

        internal override void AddRoundtripSample(ulong sample)
        {

        }

        internal override void Disconnect(bool sendMessage)
        {
            if (OnDisconnect != null)
            {
                OnDisconnect();
            }
        }

        internal override void SendRaw(ArraySegment<byte> payload, bool noMerge, ushort headerSize)
        {
            if (OnSendData != null)
            {
                OnSendData(payload);
            }
        }
    }
}
