using System;
using System.Net;
using System.Text;
using Ruffles.Channeling;
using Ruffles.Configuration;
using Ruffles.Core;

namespace Ruffles.Example
{
    public class Program
    {
        internal static readonly SocketConfig ServerConfig = new SocketConfig()
        {
            ChallengeDifficulty = 25, // Difficulty 25 is fairly hard
            ChannelTypes = new ChannelType[]
            {
                ChannelType.Reliable,
                ChannelType.ReliableSequenced,
                ChannelType.Unreliable,
                ChannelType.UnreliableSequenced
            },
            DualListenPort = 5674
        };

        internal static readonly SocketConfig ClientConfig = new SocketConfig()
        {
            ChallengeDifficulty = 25, // Difficulty 25 is fairly hard
            DualListenPort = 0 // Port 0 means we get a port by the operating system
        };


        public static void Main(string[] args)
        {
            // Uncomment the thread mode you want to run.

            NoRufflesManager();
            // RufflesManagerSingleThreaded();
            // RufflesManagerThreaded();
        }

        private static void NoRufflesManager()
        {
            RuffleSocket server = new RuffleSocket(ServerConfig);

            RuffleSocket client = new RuffleSocket(ClientConfig);

            // IPv4 Connect
            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));

            // IPv6 Connect
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));

            // The server stores the clients id here
            ulong clientId = 0;
            // The client stores the servers id here
            ulong serverId = 0;

            // The time when the connection started
            DateTime started = DateTime.Now;

            // The time when the last message was sent
            DateTime lastSent = DateTime.MinValue;

            // The amount of message that has been received
            int messagesReceived = 0;

            // The amount of messages that has been sent
            int messageCounter = 0;

            while (true)
            {
                // Runs all the internals
                client.RunInternalLoop();
                // Runs all the internals
                server.RunInternalLoop();

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
                    }

                    if (serverEvent.Type == NetworkEventType.Disconnect || serverEvent.Type == NetworkEventType.Timeout)
                    {
                        serverEvent.Connection.Recycle();
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
                        serverId = clientEvent.Connection.Id;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Got message: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"");
                        clientEvent.Recycle();
                    }

                    if (clientEvent.Type == NetworkEventType.Disconnect || clientEvent.Type == NetworkEventType.Timeout)
                    {
                        clientEvent.Connection.Recycle();
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalSeconds >= 1)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("This message was sent over a reliable channel" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("This message was sent over a reliable sequenced channel" + messageCounter);

                    server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0, false);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1, false);

                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }

        private static void RufflesManagerSingleThreaded()
        {
            // Create a ruffles manager
            RufflesManager manager = new RufflesManager(false);

            RuffleSocket server = manager.AddSocket(ServerConfig);

            RuffleSocket client = manager.AddSocket(ClientConfig);

            // IPv4 Connect
            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));

            // IPv6 Connect
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));

            // The server stores the clients id here
            ulong clientId = 0;
            // The client stores the servers id here
            ulong serverId = 0;

            // The time when the connection started
            DateTime started = DateTime.Now;

            // The time when the last message was sent
            DateTime lastSent = DateTime.MinValue;

            // The amount of message that has been received
            int messagesReceived = 0;

            // The amount of messages that has been sent
            int messageCounter = 0;

            while (true)
            {
                // Runs all the internals
                manager.RunInternals();

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
                    }

                    if (serverEvent.Type == NetworkEventType.Disconnect || serverEvent.Type == NetworkEventType.Timeout)
                    {
                        serverEvent.Connection.Recycle();
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
                        serverId = clientEvent.Connection.Id;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Got message: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"");
                        clientEvent.Recycle();
                    }

                    if (clientEvent.Type == NetworkEventType.Disconnect || clientEvent.Type == NetworkEventType.Timeout)
                    {
                        clientEvent.Connection.Recycle();
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalSeconds >= 1)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("This message was sent over a reliable channel" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("This message was sent over a reliable sequenced channel" + messageCounter);

                    server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0, false);
                    server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1, false);

                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }

        private static void RufflesManagerThreaded()
        {
            // Create a ruffles manager
            RufflesManager manager = new RufflesManager(true);

            RuffleSocket server = manager.AddSocket(ServerConfig);

            RuffleSocket client = manager.AddSocket(ClientConfig);

            // IPv4 Connect
            //client.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5674));

            // IPv6 Connect
            client.Connect(new IPEndPoint(IPAddress.Parse("0:0:0:0:0:0:0:1"), 5674));

            // The server stores the clients id here
            ulong clientId = 0;
            // The client stores the servers id here
            ulong serverId = 0;

            // The time when the connection started
            DateTime started = DateTime.Now;

            // The time when the last message was sent
            DateTime lastSent = DateTime.MinValue;

            // The amount of message that has been received
            int messagesReceived = 0;

            // The amount of messages that has been sent
            int messageCounter = 0;

            while (true)
            {
                NetworkEvent serverEvent;
                NetworkEvent clientEvent;

                lock (manager.ThreadLock)
                {
                    // Polls server for events
                    serverEvent = server.Poll();
                    // Polls client for events
                    clientEvent = client.Poll();
                }


                if (serverEvent.Type != NetworkEventType.Nothing)
                {
                    if (serverEvent.Type != NetworkEventType.Data)
                    {
                        Console.WriteLine("ServerEvent: " + serverEvent.Type);
                    }

                    if (serverEvent.Type == NetworkEventType.Connect)
                    {
                        clientId = serverEvent.Connection.Id;
                    }

                    if (serverEvent.Type == NetworkEventType.Disconnect || serverEvent.Type == NetworkEventType.Timeout)
                    {
                        lock (manager.ThreadLock)
                        {
                            serverEvent.Connection.Recycle();
                        }
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
                        serverId = clientEvent.Connection.Id;
                    }

                    if (clientEvent.Type == NetworkEventType.Data)
                    {
                        messagesReceived++;
                        Console.WriteLine("Got message: \"" + Encoding.ASCII.GetString(clientEvent.Data.Array, clientEvent.Data.Offset, clientEvent.Data.Count) + "\"");

                        lock (manager.ThreadLock)
                        {
                            clientEvent.Recycle();
                        }
                    }

                    if (clientEvent.Type == NetworkEventType.Disconnect || clientEvent.Type == NetworkEventType.Timeout)
                    {
                        lock (manager.ThreadLock)
                        {
                            clientEvent.Connection.Recycle();
                        }
                    }
                }

                if ((DateTime.Now - started).TotalSeconds > 10 && (DateTime.Now - lastSent).TotalSeconds >= 1)
                {
                    byte[] helloReliable = Encoding.ASCII.GetBytes("This message was sent over a reliable channel" + messageCounter);
                    byte[] helloReliableSequenced = Encoding.ASCII.GetBytes("This message was sent over a reliable sequenced channel" + messageCounter);

                    lock (manager.ThreadLock)
                    {
                        server.Send(new ArraySegment<byte>(helloReliableSequenced, 0, helloReliableSequenced.Length), clientId, 0, false);
                        server.Send(new ArraySegment<byte>(helloReliable, 0, helloReliable.Length), clientId, 1, false);
                    }

                    messageCounter++;
                    lastSent = DateTime.Now;
                }
            }
        }
    }
}
