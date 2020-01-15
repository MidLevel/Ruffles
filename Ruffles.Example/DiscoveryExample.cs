using System;
using System.Net;
using Ruffles.Core;

namespace Ruffles.Example
{
    public static class DiscoveryExample
    {
        public static void Server()
        {
            RuffleSocket socket = new RuffleSocket(new Configuration.SocketConfig()
            {
                AllowBroadcasts = true,
                AllowUnconnectedMessages = true,
                DualListenPort = 5555
            });

            socket.Start();

            while (true)
            {
                // Wait for message. This is to prevent a tight loop
                socket.SyncronizationEvent.WaitOne(1000);

                NetworkEvent @event;
                while ((@event = socket.Poll()).Type != NetworkEventType.Nothing)
                {
                    if (@event.Type == NetworkEventType.BroadcastData)
                    {
                        // We got a broadcast. Reply to them with the same token they used
                        socket.SendUnconnected(@event.Data, (IPEndPoint)@event.EndPoint);
                    }

                    // Recycle the event
                    @event.Recycle();
                }
            }
        }

        public static void Client()
        {
            RuffleSocket socket = new RuffleSocket(new Configuration.SocketConfig()
            {
                AllowBroadcasts = true,
                AllowUnconnectedMessages = true,
                DualListenPort = 0
            });

            socket.Start();

            // Wait for message. This is to prevent a tight loop
            socket.SyncronizationEvent.WaitOne(1000);

            // Create RNG
            System.Random rnd = new System.Random();
            // Alloc token
            byte[] token = new byte[32];
            // Fill buffer with random data
            rnd.NextBytes(token);

            // Save last send time
            DateTime lastBroadcastSendTime = DateTime.MinValue;

            while (true)
            {
                // If we havent sent broadcast for 5 seconds
                if ((DateTime.Now - lastBroadcastSendTime).TotalSeconds > 5)
                {
                    lastBroadcastSendTime = DateTime.Now;

                    // Send broadcast
                    socket.SendBroadcast(new ArraySegment<byte>(token), 5555);
                }

                // Wait for message. This is to prevent a tight loop
                socket.SyncronizationEvent.WaitOne(1000);

                NetworkEvent @event;
                while ((@event = socket.Poll()).Type != NetworkEventType.Nothing)
                {
                    if (@event.Type == NetworkEventType.UnconnectedData)
                    {
                        // We got a reply. Ensure the token is correct
                        if (@event.Data.Count == token.Length)
                        {
                            bool missMatch = false;

                            // The token had the same length. Check all elements
                            for (int i = 0; i < @event.Data.Count; i++)
                            {
                                if (@event.Data.Array[@event.Data.Offset + i] != token[i])
                                {
                                    // This element did not match. Exit
                                    missMatch = true;
                                    break;
                                }
                            }

                            if (missMatch)
                            {
                                // Continue the receive loop the loop
                                continue;
                            }
                            else
                            {
                                // All matched.
                                Console.WriteLine("Found server at endpoint: " + ((IPEndPoint)@event.EndPoint));
                            }
                        }
                    }

                    // Recycle the event
                    @event.Recycle();
                }
            }
        }
    }
}
