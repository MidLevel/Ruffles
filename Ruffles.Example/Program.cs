﻿using System;
using System.Net;
using System.Text;
using Ruffles.Channeling;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Core;

namespace Ruffles.Example
{
    public class Program
    {
        internal static readonly SocketConfig ServerConfig = new SocketConfig()
        {
            ChallengeDifficulty = 20, // Difficulty 20 is fairly hard
            ChannelTypes = new ChannelType[]
            {
                ChannelType.Reliable,
                ChannelType.ReliableSequenced,
                ChannelType.Unreliable,
                ChannelType.UnreliableOrdered,
                ChannelType.ReliableSequencedFragmented
            },
            DualListenPort = 5674,
            SimulatorConfig = new Simulation.SimulatorConfig()
            {
                DropPercentage = 0.05f,
                MaxLatency = 10,
                MinLatency = 0
            },
            UseSimulator = true
        };

        internal static readonly SocketConfig ClientConfig = new SocketConfig()
        {
            ChallengeDifficulty = 20, // Difficulty 20 is fairly hard
            DualListenPort = 0, // Port 0 means we get a port by the operating system
            SimulatorConfig = new Simulation.SimulatorConfig()
            {
                DropPercentage = 0.05f,
                MaxLatency = 10,
                MinLatency = 0
            },
            UseSimulator = true
        };

        // Can be turned off for systems that does not support IPv6
        static bool IPv6 = true;

        public static void Main(string[] args)
        {
            RuffleSocket server = new RuffleSocket(ServerConfig);

            RuffleSocket client = new RuffleSocket(ClientConfig);

            client.Start();
            server.Start();

            if (IPv6)
            {
                // IPv6 Connect
                client.ConnectNow(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));
            }
            else
            {
                // IPv4 Connect
                client.ConnectNow(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));
            }

            // The server stores the clients id here
            ulong clientId = 0;
            Connection clientConnection = null;
            // The client stores the servers id here
            ulong serverId = 0;
            Connection serverConnection = null;

            // The time when the connection started
            DateTime started = DateTime.Now;

            // The time when the last message was sent
            DateTime lastSent = DateTime.MinValue;

            // The time the last status was printed
            DateTime lastStatusPrint = DateTime.MinValue;

            // The amount of message that has been received
            int messagesReceived = 0;

            // The amount of messages that has been sent
            int messageCounter = 0;

            while (true)
            {
                // Polls server for events
                NetworkEvent serverEvent = server.Poll();
                // Polls client for events
                NetworkEvent clientEvent = client.Poll();

                if (serverEvent.Type != NetworkEventType.Nothing)
                {
                    if (serverEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ServerEvent: " + serverEvent.Type);
                    }

                    if (serverEvent.Type == NetworkEventType.Connect)
                    {
                        clientId = serverEvent.Connection.Id;
                        clientConnection = serverEvent.Connection;
                    }
                }

                serverEvent.Recycle();

                if (clientEvent.Type != NetworkEventType.Nothing)
                {
                    if (clientEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ClientEvent: " + clientEvent.Type);
                    }

                    if (clientEvent.Type == NetworkEventType.Connect)
                    {
                        serverId = clientEvent.Connection.Id;
                        serverConnection = clientEvent.Connection;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Got message: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"");
                    }
                }

                clientEvent.Recycle();

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalSeconds >= (1f / 1))
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("This message was sent over a reliable channel" + messageCounter);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1, false);
                    Console.WriteLine("Sending packet: " + messageCounter);

                    messageCounter++;
                    lastSent = DateTime.Now;
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastStatusPrint).TotalSeconds >= 5)
                {
                    Console.WriteLine("Ping: " + serverConnection.SmoothRoundtrip + "ms, " + clientConnection.SmoothRoundtrip + "ms");
                    lastStatusPrint = DateTime.Now;
                }
            }
        }
    }
}
