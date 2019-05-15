using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Hashing;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Random;
using Ruffles.Simulation;
using Ruffles.Threading;

// TODO: Make sure only connection receiver send hails to prevent a hacked client sending messed up channel configs and the receiver applying them.
// Might actually already be enforced via the state? Verify

namespace Ruffles.Core
{
    public class Listener
    {
        // Separate connections and pending to prevent something like a slorris attack
        private readonly Connection[] Connections;
        private readonly Dictionary<EndPoint, Connection> AddressConnectionLookup = new Dictionary<EndPoint, Connection>();
        private readonly Dictionary<EndPoint, Connection> AddressPendingConnectionLookup = new Dictionary<EndPoint, Connection>();

        internal readonly Queue<NetworkEvent> UserEventQueue = new Queue<NetworkEvent>();
        internal readonly ReaderWriterLockSlim EventQueueLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly Queue<ThreadHopEvent> ThreadHopQueue = new Queue<ThreadHopEvent>();
        private readonly ReaderWriterLockSlim ThreadHopQueueLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private ushort PendingConnections = 0;

        private Socket ipv4Socket;
        private Socket ipv6Socket;
        private static readonly bool SupportsIPv6 = Socket.OSSupportsIPv6;

        private readonly ListenerConfig config;
        private readonly NetworkSimulator simulator;
        private readonly byte[] outgoingInternalBuffer;
        private readonly byte[] incomingBuffer;


        public Listener(ListenerConfig config)
        {
            this.config = config;

            if (config.UseSimulator)
            {
                simulator = new NetworkSimulator(config.SimulatorConfig, SendRawReal);
            }

            outgoingInternalBuffer = new byte[Math.Max((int)config.AmplificationPreventionHandshakePadding, 128)];
            incomingBuffer = new byte[config.MaxBufferSize];
            Connections = new Connection[config.MaxConnections];

            bool bindSuccess = Bind(config.IPv4ListenAddress, config.IPv6ListenAddress, config.DualListenPort, config.UseIPv6Dual);
        }

        private bool Bind(IPAddress addressIPv4, IPAddress addressIPv6, int port, bool ipv6Dual)
        {
            // Create IPv4 UDP Socket
            ipv4Socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

            // Setup IPv4 Socket and properly bind it to the OS
            if (!SetupAndBind(ipv4Socket, new IPEndPoint(addressIPv4, port)))
            {
                // Failed to bind socket
                return false;
            }

            int ipv4LocalPort = ((IPEndPoint)ipv4Socket.LocalEndPoint).Port;

            if (!ipv6Dual || !SupportsIPv6)
            {
                // Dont use IPv6 dual mode
                return true;
            }

            // Create IPv6 UDP Socket
            ipv6Socket = new Socket(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp);

            // Setup IPv6 socket and bind it to the same port as the IPv4 socket was bound to.
            // Ignore if it fails
            SetupAndBind(ipv6Socket, new IPEndPoint(addressIPv6, ipv4LocalPort));

            return true;
        }

        private bool SetupAndBind(Socket socket, IPEndPoint endpoint)
        {
            // Dont fragment is only supported on IPv4
            if (socket.AddressFamily == AddressFamily.InterNetwork)
            {
                try
                {
                    socket.DontFragment = true;
                }
                catch (SocketException)
                {
                    // TODO: Handle
                    // This shouldnt happen when the OS supports it.
                    // This is used for path MTU to do application level fragmentation
                }
            }

            try
            {
                // Bind the socket to the OS
                socket.Bind(endpoint);
            }
            catch (SocketException bindException)
            {
                switch (bindException.SocketErrorCode)
                {
                    // IPv6 bind fix
                    case SocketError.AddressAlreadyInUse:
                        {
                            if (socket.AddressFamily == AddressFamily.InterNetworkV6)
                            {
                                try
                                {
                                    socket.SetSocketOption(SocketOptionLevel.IPv6, (SocketOptionName)27, true);
                                    socket.Bind(endpoint);
                                }
                                catch (SocketException)
                                {
                                    // TODO: Handle
                                    return false;
                                }

                                return true;
                            }
                        }
                        break;
                    // Fixes Unity exception for iOS (requires IPv6 but the runtime throws)
                    case SocketError.AddressFamilyNotSupported:
                        {
                            return true;
                        }
                }

                return false;
            }

            return true;
        }

        public void Send(ArraySegment<byte> payload, ulong id, byte channelId)
        {
            if (config.EnableThreadSafety)
            {
                // Alloc some memory
                HeapMemory memory = MemoryManager.Alloc(payload.Count);

                // Copy payload
                Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 0, payload.Count);

                ThreadHopQueueLock.EnterWriteLock();
                try
                {
                    ThreadHopQueue.Enqueue(new ThreadHopEvent()
                    {
                        Type = ThreadHopType.Send,
                        ConnectionId = id,
                        ChannelId = channelId,
                        Memory = memory,
                    });
                }
                finally
                {
                    ThreadHopQueueLock.ExitWriteLock();
                }
            }
            else
            {
                // TODO: Safety
                PacketHandler.SendMessage(payload, Connections[id], channelId);
            }

        }

        public void Connect(EndPoint endpoint)
        {
            if (config.EnableThreadSafety)
            {
                ThreadHopQueueLock.EnterWriteLock();
                try
                {
                    ThreadHopQueue.Enqueue(new ThreadHopEvent()
                    {
                        Type = ThreadHopType.Connect,
                        Endpoint = endpoint
                    });
                }
                finally
                {
                    ThreadHopQueueLock.ExitWriteLock();
                }
            }
            else
            {
                Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingConnection);

                if (connection != null)
                {
                    // Set resend values
                    connection.HandshakeResendAttempts = 1;
                    connection.HandshakeLastSendTime = DateTime.Now;

                    outgoingInternalBuffer[0] = (byte)MessageType.ConnectionRequest;

                    connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1, (int)config.AmplificationPreventionHandshakePadding)));
                }
            }
        }

        public void Disconnect(ulong id)
        {
            if (config.EnableThreadSafety)
            {
                ThreadHopQueueLock.EnterWriteLock();
                try
                {
                    ThreadHopQueue.Enqueue(new ThreadHopEvent()
                    {
                        Type = ThreadHopType.Send,
                        ConnectionId = id,
                        ChannelId = 0,
                        Memory = null,
                    });
                }
                finally
                {
                    ThreadHopQueueLock.ExitWriteLock();
                }
            }
            else
            {
                // TODO: Safety
                DisconnectConnection(Connections[id], true);
            }
        }


        private DateTime _lastTimeoutCheckRan = DateTime.MinValue;

        public void RunInternalLoop()
        {
            if (config.EnableThreadSafety)
            {
                RunUserHops();
            }

            InternalPollSocket();

            if (simulator != null)
            {
                simulator.RunLoop();
            }

            // Run timeout loop once every ConnectionPollDelay ms
            if ((DateTime.Now - _lastTimeoutCheckRan).TotalMilliseconds >= config.MinConnectionPollDelay)
            {
                CheckConnectionTimeouts();
                CheckConnectionHeartbeats();
                CheckConnectionResends();
                RunChannelInternalUpdate();
                _lastTimeoutCheckRan = DateTime.Now;
            }
        }

        private void RunUserHops()
        {
            ThreadHopQueueLock.EnterWriteLock();
            try
            {
                while (ThreadHopQueue.Count > 0)
                {
                    ThreadHopEvent @event = ThreadHopQueue.Dequeue();

                    switch (@event.Type)
                    {
                        case ThreadHopType.Connect:
                            {
                                Connection connection = AddNewConnection(@event.Endpoint, ConnectionState.RequestingConnection);

                                if (connection != null)
                                {
                                    // Set resend values
                                    connection.HandshakeResendAttempts = 1;
                                    connection.HandshakeLastSendTime = DateTime.Now;

                                    outgoingInternalBuffer[0] = (byte)MessageType.ConnectionRequest;

                                    connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1, (int)config.AmplificationPreventionHandshakePadding)));
                                }
                            }
                            break;
                        case ThreadHopType.Disconnect:
                            {
                                // TODO: Safety
                                DisconnectConnection(Connections[@event.ConnectionId], true);
                            }
                            break;
                        case ThreadHopType.Send:
                            {
                                // TODO: Safety
                                PacketHandler.SendMessage(new ArraySegment<byte>(@event.Memory.Buffer, @event.Memory.VirtualOffset, @event.Memory.VirtualCount), Connections[@event.ConnectionId], @event.ChannelId);

                                // Dealloc the message memory
                                MemoryManager.DeAlloc(@event.Memory);
                            }
                            break;
                    }
                }
            }
            finally
            {
                ThreadHopQueueLock.ExitWriteLock();
            }
        }

        private void RunChannelInternalUpdate()
        {
            for (int i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] != null && !Connections[i].Dead && Connections[i].Channels != null)
                {
                    for (int x = 0; x < Connections[i].Channels.Length; x++)
                    {
                        if (Connections[i].Channels[x] != null)
                        {
                            Connections[i].Channels[x].InternalUpdate();
                        }
                    }
                }
            }
        }

        private void CheckConnectionResends()
        {
            for (int i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] != null && !Connections[i].Dead)
                {
                    if (Connections[i].State == ConnectionState.RequestingConnection)
                    {
                        if ((DateTime.Now - Connections[i].HandshakeLastSendTime).TotalMilliseconds > config.ConnectionRequestMinResendDelay && Connections[i].HandshakeResendAttempts < config.MaxConnectionRequestResends)
                        {
                            Connections[i].HandshakeResendAttempts++;
                            Connections[i].HandshakeLastSendTime = DateTime.Now;

                            outgoingInternalBuffer[0] = (byte)MessageType.ConnectionRequest;

                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1, (int)config.AmplificationPreventionHandshakePadding)));
                        }
                    }
                    else if (Connections[i].State == ConnectionState.RequestingChallenge)
                    {
                        if ((DateTime.Now - Connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeMinResendDelay && Connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            Connections[i].HandshakeResendAttempts++;
                            Connections[i].HandshakeLastSendTime = DateTime.Now;

                            // Write connection challenge
                            outgoingInternalBuffer[0] = (byte)MessageType.ChallengeRequest;
                            for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + x] = ((byte)(Connections[i].ConnectionChallenge >> (x * 8)));
                            outgoingInternalBuffer[1 + sizeof(ulong)] = Connections[i].ChallengeDifficulty;
                            
                            // Send the challenge
                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1 + sizeof(ulong) + 1));
                        }
                    }
                    else if (Connections[i].State == ConnectionState.SolvingChallenge)
                    {
                        if ((DateTime.Now - Connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeMinResendDelay && Connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            Connections[i].HandshakeResendAttempts++;
                            Connections[i].HandshakeLastSendTime = DateTime.Now;

                            // Write the challenge response
                            outgoingInternalBuffer[0] = (byte)MessageType.ChallengeResponse;
                            for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + x] = ((byte)(Connections[i].ChallengeAnswer >> (x * 8)));
                            
                            // Send the challenge response
                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1 + sizeof(ulong), (int)config.AmplificationPreventionHandshakePadding)));
                        }
                    }
                    else if (Connections[i].State == ConnectionState.Connected)
                    {
                        if (!Connections[i].HailStatus.Completed && (DateTime.Now - Connections[i].HailStatus.LastAttempt).TotalMilliseconds > config.HandshakeMinResendDelay && Connections[i].HailStatus.Attempts < config.MaxHandshakeResends)
                        {
                            // Send the response
                            outgoingInternalBuffer[0] = (byte)MessageType.Hail;

                            // Write the amount of channels
                            outgoingInternalBuffer[1] = (byte)config.ChannelTypes.Length;

                            // Write the channel types
                            for (byte x = 0; x < (byte)config.ChannelTypes.Length; x++)
                            {
                                outgoingInternalBuffer[2 + x] = (byte)config.ChannelTypes[x];
                            }

                            Connections[i].HailStatus.Attempts++;
                            Connections[i].HailStatus.LastAttempt = DateTime.Now;

                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 2 + (byte)config.ChannelTypes.Length));
                        }
                    }
                }
            }
        }

        private void CheckConnectionTimeouts()
        {
            for (int i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] != null && !Connections[i].Dead)
                {
                    if (Connections[i].State != ConnectionState.Connected)
                    {
                        if ((DateTime.Now - Connections[i].ConnectionStarted).TotalMilliseconds > config.HandshakeTimeout)
                        {
                            // This client has taken too long to connect. Let it go.
                            DisconnectConnection(Connections[i], false);

                            // Send to userspace
                            EventQueueLock.EnterWriteLock();
                            try
                            {
                                UserEventQueue.Enqueue(new NetworkEvent()
                                {
                                    ConnectionId = Connections[i].Id,
                                    Listener = this,
                                    Type = NetworkEventType.Timeout
                                });
                            }
                            finally
                            {
                                EventQueueLock.ExitWriteLock();
                            }
                        }
                    }
                    else
                    {
                        if ((DateTime.Now - Connections[i].LastMessageIn).TotalMilliseconds > config.ConnectionTimeout)
                        {
                            // This client has not answered us in way too long. Let it go
                            DisconnectConnection(Connections[i], false);

                            // Send to userspace
                            EventQueueLock.EnterWriteLock();
                            try
                            {
                                UserEventQueue.Enqueue(new NetworkEvent()
                                {
                                    ConnectionId = Connections[i].Id,
                                    Listener = this,
                                    Type = NetworkEventType.Timeout
                                });
                            }
                            finally
                            {
                                EventQueueLock.ExitWriteLock();
                            }
                        }
                    }
                }
            }
        }

        private void CheckConnectionHeartbeats()
        {
            for (int i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] != null && !Connections[i].Dead && Connections[i].State == ConnectionState.Connected)
                {
                    if ((DateTime.Now - Connections[i].LastMessageOut).TotalMilliseconds > config.MinHeartbeatDelay)
                    {
                        // This client has not been talked to in a long time. Send a heartbeat.

                        // Create sequenced heartbeat packet
                        HeapMemory heartbeatMemory = Connections[i].HeartbeatChannel.CreateOutgoingHeartbeatMessage();

                        // Send heartbeat
                        Connections[i].SendRaw(new ArraySegment<byte>(heartbeatMemory.Buffer, heartbeatMemory.VirtualOffset, heartbeatMemory.VirtualCount));

                        // DeAlloc the memory
                        MemoryManager.DeAlloc(heartbeatMemory);
                    }
                }
            }
        }

        private EndPoint _fromIPv4Endpoint = new IPEndPoint(IPAddress.Any, 0);
        private EndPoint _fromIPv6Endpoint = new IPEndPoint(IPAddress.IPv6Any, 0);
        private void InternalPollSocket()
        {
            if (ipv4Socket != null && ipv4Socket.Poll(config.MaxSocketBlockMilliseconds * 1000, SelectMode.SelectRead))
            {
                // TODO: Handle SocketException when buffer is too small.
                int size = ipv4Socket.ReceiveFrom(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ref _fromIPv4Endpoint);

                HandlePacket(incomingBuffer, size, _fromIPv4Endpoint);
            }

            if (ipv6Socket != null && ipv6Socket.Poll(config.MaxSocketBlockMilliseconds * 1000, SelectMode.SelectRead))
            {
                // TODO: Handle SocketException when buffer is too small.
                int size = ipv6Socket.ReceiveFrom(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ref _fromIPv6Endpoint);

                HandlePacket(incomingBuffer, size, _fromIPv6Endpoint);
            }
        }

        public NetworkEvent Poll()
        {
            EventQueueLock.EnterWriteLock();
            try
            {
                if (UserEventQueue.Count > 0)
                {
                    NetworkEvent @event = UserEventQueue.Dequeue();

                    return @event;
                }
                else
                {
                    return new NetworkEvent()
                    {
                        ConnectionId = 0,
                        Listener = this,
                        Type = NetworkEventType.Nothing
                    };
                }
            }
            finally
            {
                EventQueueLock.ExitWriteLock();
            }
        }

        internal void SendRaw(Connection connection, ArraySegment<byte> payload)
        {
            connection.LastMessageOut = DateTime.Now;

            if (simulator != null)
            {
                simulator.Add(connection, payload);
            }
            else
            {
                SendRawReal(connection, payload);
            }
        }

        private void SendRawReal(Connection connection, ArraySegment<byte> payload)
        {
            if (connection.EndPoint.AddressFamily == AddressFamily.InterNetwork)
            {
                int sent = ipv4Socket.SendTo(payload.Array, payload.Offset, payload.Count, SocketFlags.None, connection.EndPoint);
            }
            else if (connection.EndPoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                int sent = ipv6Socket.SendTo(payload.Array, payload.Offset, payload.Count, SocketFlags.None, connection.EndPoint);
            }
        }

        internal void HandlePacket(byte[] payload, int size, EndPoint endpoint)
        {
            if (size < 1)
            {
                // Invalid size
                return;
            }

            // Don't cast to enum for safety
            byte messageType = payload[0];

            switch (messageType)
            {
                case (byte)MessageType.ConnectionRequest: // Connection Request
                    {
                        if (size < config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification. 
                            return;
                        }

                        Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingChallenge);

                        if (connection != null)
                        {
                            // This connection was successfully added as pending

                            // Set resend values
                            connection.HandshakeResendAttempts = 1;
                            connection.HandshakeLastSendTime = DateTime.Now;

                            // Write connection challenge
                            outgoingInternalBuffer[0] = (byte)MessageType.ChallengeRequest;
                            for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + i] = ((byte)(connection.ConnectionChallenge >> (i * 8)));
                            outgoingInternalBuffer[1 + sizeof(ulong)] = connection.ChallengeDifficulty;

                            // Send the challenge
                            connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1 + sizeof(ulong) + 1));
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeRequest:
                    {
                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingConnection)
                        {
                            if (size < 10)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                DisconnectConnection(connection, false);
                                return;
                            }

                            connection.LastMessageIn = DateTime.Now;

                            connection.ConnectionChallenge = (((ulong)payload[1 + 0]) |
                                                                ((ulong)payload[1 + 1] << 8) |
                                                                ((ulong)payload[1 + 2] << 16) |
                                                                ((ulong)payload[1 + 3] << 24) |
                                                                ((ulong)payload[1 + 4] << 32) |
                                                                ((ulong)payload[1 + 5] << 40) |
                                                                ((ulong)payload[1 + 6] << 48) |
                                                                ((ulong)payload[1 + 7] << 56));

                            connection.ChallengeDifficulty = payload[1 + sizeof(ulong)];

                            ulong collidedValue = connection.ConnectionChallenge;
                            ulong additionsRequired = 0;

                            // Solve the hashcash
                            while (connection.ChallengeDifficulty > 0 && ((collidedValue << ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) >> ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) != 0)
                            {
                                additionsRequired++;
                                collidedValue = HashProvider.GetStableHash64(connection.ConnectionChallenge + additionsRequired);
                            }

                            connection.ChallengeAnswer = additionsRequired;

                            // Set resend values
                            connection.HandshakeResendAttempts = 1;
                            connection.HandshakeLastSendTime = DateTime.Now;
                            connection.State = ConnectionState.SolvingChallenge;

                            // Write the challenge response
                            outgoingInternalBuffer[0] = (byte)MessageType.ChallengeResponse;
                            for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + i] = ((byte)(additionsRequired >> (i * 8)));


                            // Send the challenge response
                            connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1 + sizeof(ulong), (int)config.AmplificationPreventionHandshakePadding)));
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeResponse:
                    {
                        if (size < config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification
                            return;
                        }

                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingChallenge)
                        {
                            if (size < 9)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                DisconnectConnection(connection, false);
                                return;
                            }

                            connection.LastMessageIn = DateTime.Now;

                            ulong challengeResponse = (((ulong)payload[1 + 0]) |
                                                        ((ulong)payload[1 + 1] << 8) |
                                                        ((ulong)payload[1 + 2] << 16) |
                                                        ((ulong)payload[1 + 3] << 24) |
                                                        ((ulong)payload[1 + 4] << 32) |
                                                        ((ulong)payload[1 + 5] << 40) |
                                                        ((ulong)payload[1 + 6] << 48) |
                                                        ((ulong)payload[1 + 7] << 56));

                            ulong claimedCollision = connection.ConnectionChallenge + challengeResponse;

                            bool isCollided = connection.ChallengeDifficulty == 0 || ((HashProvider.GetStableHash64(claimedCollision) << ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) >> ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) == 0;

                            if (isCollided)
                            {
                                // Success, they completed the hashcash challenge

                                ConnectPendingConnection(connection);

                                connection.HailStatus.Attempts = 1;
                                connection.HailStatus.HasAcked = false;
                                connection.HailStatus.LastAttempt = DateTime.Now;

                                // Send the response
                                outgoingInternalBuffer[0] = (byte)MessageType.Hail;

                                // Write the amount of channels
                                outgoingInternalBuffer[1] = (byte)config.ChannelTypes.Length;

                                // Write the channel types
                                for (byte i = 0; i < (byte)config.ChannelTypes.Length; i++)
                                {
                                    outgoingInternalBuffer[2 + i] = (byte)config.ChannelTypes[i];
                                }

                                connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 2 + (byte)config.ChannelTypes.Length));

                                // Send to userspace
                                EventQueueLock.EnterWriteLock();
                                try
                                {
                                    UserEventQueue.Enqueue(new NetworkEvent()
                                    {
                                        ConnectionId = connection.Id,
                                        Listener = this,
                                        Type = NetworkEventType.Connect
                                    });
                                }
                                finally
                                {
                                    EventQueueLock.ExitWriteLock();
                                }
                            }
                            else
                            {
                                // Failed, disconnect them
                                DisconnectConnection(connection, false);
                            }
                        }
                    }
                    break;
                case (byte)MessageType.Hail:
                    {
                        Connection pendingConnection = GetPendingConnection(endpoint);
                        Connection connectedConnection = GetConnection(endpoint);

                        if (connectedConnection != null && connectedConnection.State == ConnectionState.Connected)
                        {
                            // Send the confirmation
                            outgoingInternalBuffer[0] = (byte)MessageType.HailConfirmed;

                            // Send confirmation
                            connectedConnection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1));
                        }
                        else if (pendingConnection != null && pendingConnection.State == ConnectionState.SolvingChallenge)
                        {
                            if (size < 2)
                            {
                                // Invalid size.
                                DisconnectConnection(pendingConnection, false);
                                return;
                            }

                            pendingConnection.LastMessageIn = DateTime.Now;

                            // Read the amount of channels
                            byte channelCount = payload[1];

                            if (size < channelCount + 2)
                            {
                                // Invalid size.
                                DisconnectConnection(pendingConnection, false);
                                return;
                            }

                            // Alloc the types
                            ChannelType[] channelTypes = new ChannelType[channelCount];

                            // Read the types
                            for (byte i = 0; i < channelCount; i++)
                            {
                                byte channelType = payload[2 + i];

                                switch (channelType)
                                {
                                    case (byte)ChannelType.Reliable:
                                        {
                                            channelTypes[i] = ChannelType.Reliable;
                                        }
                                        break;
                                    case (byte)ChannelType.Unreliable:
                                        {
                                            channelTypes[i] = ChannelType.Unreliable;
                                        }
                                        break;
                                    case (byte)ChannelType.UnreliableSequenced:
                                        {
                                            channelTypes[i] = ChannelType.UnreliableSequenced;
                                        }
                                        break;
                                    case (byte)ChannelType.ReliableSequenced:
                                        {
                                            channelTypes[i] = ChannelType.ReliableSequenced;
                                        }
                                        break;
                                    default:
                                        {
                                            // Unknown channel type. Disconnect.
                                            DisconnectConnection(pendingConnection, false);
                                        }
                                        break;
                                }
                            }

                            if (pendingConnection.Channels != null)
                            {
                                for (int i = 0; i < pendingConnection.Channels.Length; i++)
                                {
                                    if (pendingConnection.Channels[i] != null)
                                    {
                                        // Free up resources and reset states.
                                        pendingConnection.Channels[i].Reset();
                                    }
                                }
                            }

                            // Alloc the channels array
                            pendingConnection.Channels = new IChannel[channelCount];

                            // Alloc the channels
                            for (byte i = 0; i < channelTypes.Length; i++)
                            {
                                switch (channelTypes[i])
                                {
                                    case ChannelType.Reliable:
                                        {
                                            pendingConnection.Channels[i] = new ReliableChannel(i, pendingConnection, config);
                                        }
                                        break;
                                    case ChannelType.Unreliable:
                                        {
                                            pendingConnection.Channels[i] = new UnreliableChannel(i, pendingConnection, config);
                                        }
                                        break;
                                    case ChannelType.UnreliableSequenced:
                                        {
                                            pendingConnection.Channels[i] = new UnreliableSequencedChannel(i, pendingConnection);
                                        }
                                        break;
                                    case ChannelType.ReliableSequenced:
                                        {
                                            pendingConnection.Channels[i] = new ReliableSequencedChannel(i, pendingConnection, this, config);
                                        }
                                        break;
                                    default:
                                        {
                                            // Unknown channel type. Disconnect.
                                            DisconnectConnection(pendingConnection, false);
                                        }
                                        break;
                                }
                            }

                            // Set state to connected
                            ConnectPendingConnection(pendingConnection);

                            EventQueueLock.EnterWriteLock();
                            try
                            {
                                UserEventQueue.Enqueue(new NetworkEvent()
                                {
                                    ConnectionId = pendingConnection.Id,
                                    Listener = this,
                                    Type = NetworkEventType.Connect
                                });
                            }
                            finally
                            {
                                EventQueueLock.ExitWriteLock();
                            }

                            // Send the confirmation
                            outgoingInternalBuffer[0] = (byte)MessageType.HailConfirmed;

                            // Send confirmation
                            pendingConnection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1));
                        }
                    }
                    break;
                case (byte)MessageType.HailConfirmed:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            if (!connection.HailStatus.Completed && connection.State == ConnectionState.Connected)
                            {
                                connection.HailStatus.HasAcked = true;
                            }
                        }
                    }
                    break;
                case (byte)MessageType.Heartbeat:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            // Heartbeats are sequenced to not properly handle network congestion

                            if (connection.HeartbeatChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(payload, 1, size - 1), out bool hasMore) != null)
                            {
                                connection.LastMessageIn = DateTime.Now;
                            }
                        }
                    }
                    break;
                case (byte)MessageType.Data:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.LastMessageIn = DateTime.Now;

                            PacketHandler.HandleIncomingMessage(new ArraySegment<byte>(payload, 1, size - 1), connection);
                        }
                    }
                    break;
                case (byte)MessageType.Ack:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.LastMessageIn = DateTime.Now;

                            byte channelId = payload[1];

                            // TODO: Safety
                            IChannel channel = connection.Channels[channelId];

                            // Handle ack
                            channel.HandleAck(new ArraySegment<byte>(payload, 2, size - 2));
                        }
                    }
                    break;
                case (byte)MessageType.Disconnect:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.Disconnect(false);
                        }
                    }
                    break;
            }
        }

        internal void ConnectPendingConnection(Connection connection)
        {
            // Remove it from pending
            AddressPendingConnectionLookup.Remove(connection.EndPoint);
            AddressConnectionLookup.Add(connection.EndPoint, connection);

            connection.State = ConnectionState.Connected;

            PendingConnections++;
        }

        internal Connection GetPendingConnection(EndPoint endpoint)
        {
            if (AddressPendingConnectionLookup.ContainsKey(endpoint))
            {
                return AddressPendingConnectionLookup[endpoint];
            }
            else
            {
                return null;
            }
        }

        internal Connection GetConnection(EndPoint endpoint)
        {
            if (AddressConnectionLookup.ContainsKey(endpoint))
            {
                return AddressConnectionLookup[endpoint];
            }
            else
            {
                return null;
            }
        }

        internal void DisconnectConnection(Connection connection, bool sendMessage)
        {
            if (connection.State == ConnectionState.Connected && sendMessage)
            {
                if (sendMessage)
                {
                    // Send disconnect message

                    // Write disconnect header
                    outgoingInternalBuffer[0] = (byte)MessageType.Disconnect;

                    // Send disconnect message
                    connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1));
                }
            }

            // Mark as dead, this will allow it to be reclaimed
            connection.Dead = true;

            // Reset all channels, releasing memory etc
            for (int i = 0; i < connection.Channels.Length; i++)
            {
                connection.Channels[i].Reset();
            }

            // Remove connection lookups
            if (connection.State != ConnectionState.Connected)
            {
                AddressPendingConnectionLookup.Remove(connection.EndPoint);

                PendingConnections--;
            }
            else
            {
                AddressConnectionLookup.Remove(connection.EndPoint);
            }
        }

        internal Connection AddNewConnection(EndPoint endpoint, ConnectionState state)
        {
            // Make sure they are not already connected to prevent an attack where a single person can fill all the slots.
            if (AddressPendingConnectionLookup.ContainsKey(endpoint) || AddressConnectionLookup.ContainsKey(endpoint) || PendingConnections > config.MaxPendingConnections)
            {
                return null;
            }

            Connection connection = null;

            for (ushort i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] == null)
                {
                    // Alloc on the heap
                    connection = new Connection
                    {
                        Dead = false,
                        Id = i,
                        State = state,
                        HailStatus = new MessageStatus(),
                        Listener = this,
                        EndPoint = endpoint,
                        ConnectionChallenge = RandomProvider.GetRandomULong(),
                        ChallengeDifficulty = config.ChallengeDifficulty,
                        LastMessageIn = DateTime.Now,
                        LastMessageOut = DateTime.Now,
                        ConnectionStarted = DateTime.Now,
                        HandshakeResendAttempts = 0,
                        ChallengeAnswer = 0,
                        Channels = new IChannel[0],
                        HandshakeLastSendTime = DateTime.Now,
                        Roundtrip = 10
                    };

                    // Make sure the array is not null
                    if (config.ChannelTypes == null)
                    {
                        config.ChannelTypes = new ChannelType[0];
                    }

                    // Alloc the channel array
                    connection.Channels = new IChannel[config.ChannelTypes.Length];

                    // Alloc the channels
                    for (byte x = 0; x < config.ChannelTypes.Length; x++)
                    {
                        switch (config.ChannelTypes[x])
                        {
                            case ChannelType.Reliable:
                                {
                                    connection.Channels[x] = new ReliableChannel(x, connection, config);
                                }
                                break;
                            case ChannelType.Unreliable:
                                {
                                    connection.Channels[x] = new UnreliableChannel(x, connection, config);
                                }
                                break;
                            case ChannelType.UnreliableSequenced:
                                {
                                    connection.Channels[x] = new UnreliableSequencedChannel(x, connection);
                                }
                                break;
                            case ChannelType.ReliableSequenced:
                                {
                                    connection.Channels[x] = new ReliableSequencedChannel(x, connection, this, config);
                                }
                                break;
                            default:
                                {
                                    // Unknown channel type. Disconnect.
                                    // TODO: Fix
                                    DisconnectConnection(connection, false);
                                }
                                break;
                        }
                    }

                    Connections[i] = connection;
                    AddressPendingConnectionLookup.Add(endpoint, connection);

                    PendingConnections++;

                    break;
                }
                else if (Connections[i].Dead)
                {
                    // This is no longer used, reuse it
                    connection = Connections[i];
                    connection.Dead = false;
                    connection.State = state;
                    connection.HailStatus = new MessageStatus();
                    connection.Id = i;
                    connection.Listener = this;
                    connection.EndPoint = endpoint;
                    connection.ConnectionChallenge = RandomProvider.GetRandomULong();
                    connection.ChallengeDifficulty = config.ChallengeDifficulty;
                    connection.LastMessageOut = DateTime.Now;
                    connection.LastMessageIn = DateTime.Now;
                    connection.ConnectionStarted = DateTime.Now;
                    connection.ChallengeAnswer = 0;
                    connection.HandshakeLastSendTime = DateTime.Now;
                    connection.Roundtrip = 10;
                    connection.HeartbeatChannel.Reset();

                    if (Connections[i].Channels != null)
                    {
                        for (int x = 0; x < Connections[i].Channels.Length; x++)
                        {
                            // Reset the channels, releasing memory etc
                            Connections[i].Channels[x].Reset();
                        }
                    }

                    AddressPendingConnectionLookup.Add(endpoint, connection);

                    PendingConnections++;

                    break;
                }
            }

            return connection;
        }
    }
}
