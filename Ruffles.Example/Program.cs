using System;
using System.Net;
using System.Text;
using Ruffles.Channeling;
using Ruffles.Configuration;
using Ruffles.Core;
using Ruffles.Simulation;

namespace Ruffles.Example
{
    class Program
    {
        static void Main(string[] args)
        {
            // Uncomment the one you want to run.


            NoRufflesManager();
            // RufflesManagerSingleThreaded();
            // RufflesManagerThreaded();
        }

        private static void NoRufflesManager()
        {
            Listener server = new Listener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 5674,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                EnableThreadSafety = false,
                ChannelTypes = new ChannelType[] { ChannelType.ReliableSequenced, ChannelType.Reliable },
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            Listener client = new Listener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 0,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                EnableThreadSafety = false,
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));


            ulong clientId = 0;
            ulong serverId = 0;

            DateTime started = DateTime.Now;
            DateTime lastSent = DateTime.MinValue;
            int messagesReceived = 0;
            int messageCounter = 0;

            while (true)
            {
                client.RunInternalLoop();
                server.RunInternalLoop();

                NetworkEvent serverEvent = server.Poll();
                NetworkEvent clientEvent = client.Poll();


                if (serverEvent.Type != NetworkEventType.Nothing)
                {
                    if (serverEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ServerEvent: " + serverEvent.Type);
                    }

                    if (serverEvent.Type == NetworkEventType.Connect)
                    {
                        clientId = serverEvent.ConnectionId;
                    }
                }

                if (clientEvent.Type != NetworkEventType.Nothing)
                {
                    if (clientEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ClientEvent: " + clientEvent.Type);
                    }

                    if (clientEvent.Type == NetworkEventType.Connect)
                    {
                        serverId = clientEvent.ConnectionId;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Payload: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"" + " Total: " + messagesReceived);
                        clientEvent.Recycle();
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalMilliseconds >= 50)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("RELIABLE HELLO WORLD" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("RELIABLE SEQUENCED HELLO WORLD" + messageCounter);

                    server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1);
                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }

        private static void RufflesManagerSingleThreaded()
        {
            RufflesManager manager = new RufflesManager(false);

            Listener server = manager.AddListener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 5674,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                EnableThreadSafety = false,
                ChannelTypes = new ChannelType[] { ChannelType.ReliableSequenced, ChannelType.Reliable },
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            Listener client = manager.AddListener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 0,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                EnableThreadSafety = false,
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));

            ulong clientId = 0;
            ulong serverId = 0;

            DateTime started = DateTime.Now;
            DateTime lastSent = DateTime.MinValue;
            int messagesReceived = 0;
            int messageCounter = 0;

            while (true)
            {
                NetworkEvent serverEvent = server.Poll();
                NetworkEvent clientEvent = client.Poll();

                // We need to run all internals manually since we never poll the manager, only the listeners themselves.
                manager.RunInternals();


                if (serverEvent.Type != NetworkEventType.Nothing)
                {
                    if (serverEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ServerEvent: " + serverEvent.Type);
                    }

                    if (serverEvent.Type == NetworkEventType.Connect)
                    {
                        clientId = serverEvent.ConnectionId;
                    }
                }

                if (clientEvent.Type != NetworkEventType.Nothing)
                {
                    if (clientEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ClientEvent: " + clientEvent.Type);
                    }

                    if (clientEvent.Type == NetworkEventType.Connect)
                    {
                        serverId = clientEvent.ConnectionId;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Payload: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"" + " Total: " + messagesReceived);
                        clientEvent.Recycle();
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalMilliseconds >= 50)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("RELIABLE HELLO WORLD" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("RELIABLE SEQUENCED HELLO WORLD" + messageCounter);

                    server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1);
                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }

        private static void RufflesManagerThreaded()
        {
            RufflesManager manager = new RufflesManager(true);

            Listener server = manager.AddListener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 5674,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                ChannelTypes = new ChannelType[] { ChannelType.ReliableSequenced, ChannelType.Reliable },
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            Listener client = manager.AddListener(new ListenerConfig()
            {
                ChallengeDifficulty = 25,
                IPv4ListenAddress = IPAddress.Any,
                IPv6ListenAddress = IPAddress.IPv6Any,
                DualListenPort = 0,
                MaxBufferSize = 4096,
                MaxConnections = 200,
                MaxPendingConnections = 200,
                MaxSocketBlockMilliseconds = 5,
                MinConnectionPollDelay = 50,
                ConnectionTimeout = 60000,
                HandshakeMinResendDelay = 1000,
                MaxConnectionRequestResends = 100,
                MinHeartbeatDelay = 5000,
                HandshakeTimeout = 60000,
                ConnectionRequestMinResendDelay = 1000,
                MaxHandshakeResends = 100,
                UseSimulator = true,
                SimulatorConfig = new SimulatorConfig()
                {
                    DropPercentage = 0.1f,
                    MaxLatency = 300,
                    MinLatency = 50
                }
            });

            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));

            ulong clientId = 0;
            ulong serverId = 0;

            DateTime started = DateTime.Now;
            DateTime lastSent = DateTime.MinValue;
            int messagesReceived = 0;
            int messageCounter = 0;

            while (true)
            {
                NetworkEvent serverEvent = server.Poll();
                NetworkEvent clientEvent = client.Poll();


                if (serverEvent.Type != NetworkEventType.Nothing)
                {
                    if (serverEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ServerEvent: " + serverEvent.Type);
                    }

                    if (serverEvent.Type == NetworkEventType.Connect)
                    {
                        clientId = serverEvent.ConnectionId;
                    }
                }

                if (clientEvent.Type != NetworkEventType.Nothing)
                {
                    if (clientEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ClientEvent: " + clientEvent.Type);
                    }

                    if (clientEvent.Type == NetworkEventType.Connect)
                    {
                        serverId = clientEvent.ConnectionId;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Payload: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"" + " Total: " + messagesReceived);
                        clientEvent.Recycle();
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalMilliseconds >= 50)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("RELIABLE HELLO WORLD" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("RELIABLE SEQUENCED HELLO WORLD" + messageCounter);

                    server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1);
                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }
    }
}
