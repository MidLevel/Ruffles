using System;
using System.Collections.Generic;
using Ruffles.Connections;

namespace Ruffles.Simulation
{
    internal class NetworkSimulator
    {
        internal struct OutgoingPacket
        {
            public byte[] Data;
            public Connection Connection;
        }

        internal delegate void SendDelegate(Connection connection, ArraySegment<byte> payload);

        private readonly System.Random random = new System.Random();
        private readonly SortedList<DateTime, OutgoingPacket> _packets = new SortedList<DateTime, OutgoingPacket>();
        private readonly SimulatorConfig config;
        private readonly SendDelegate sendDelegate;

        internal NetworkSimulator(SimulatorConfig config, SendDelegate sendDelegate)
        {
            this.config = config;
            this.sendDelegate = sendDelegate;
        }

        internal void Add(Connection connection, ArraySegment<byte> payload)
        {
            if (random.NextDouble() < (double)config.DropPercentage)
            {
                // Packet drop
                return;
            }

            byte[] garbageAlloc = new byte[payload.Count];
            Buffer.BlockCopy(payload.Array, payload.Offset, garbageAlloc, 0, payload.Count);


            DateTime scheduledTime;
            do
            {
                scheduledTime = DateTime.Now.AddMilliseconds(random.Next(config.MinLatency, config.MaxLatency));
            }
            while (_packets.ContainsKey(scheduledTime));

            _packets.Add(scheduledTime, new OutgoingPacket()
            {
                Data = garbageAlloc,
                Connection = connection
            });
        }

        internal void RunLoop()
        {
            while (_packets.Keys.Count > 0 && DateTime.Now >= _packets.Keys[0])
            {
                sendDelegate(_packets[_packets.Keys[0]].Connection, new ArraySegment<byte>(_packets[_packets.Keys[0]].Data, 0, _packets[_packets.Keys[0]].Data.Length));
                _packets.RemoveAt(0);
            }
        }
    }
}
