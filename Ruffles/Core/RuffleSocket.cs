﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Hashing;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Random;
using Ruffles.Simulation;
using Ruffles.Utils;

// TODO: Make sure only connection receiver send hails to prevent a hacked client sending messed up channel configs and the receiver applying them.
// Might actually already be enforced via the state? Verify

namespace Ruffles.Core
{
    /// <summary>
    /// A dual IPv4 IPv6 socket using the Ruffles protocol.
    /// </summary>
    public class RuffleSocket
    {
        // Separate connections and pending to prevent something like a slorris attack
        private readonly Connection[] Connections;
        private readonly Dictionary<EndPoint, Connection> AddressConnectionLookup = new Dictionary<EndPoint, Connection>();
        private readonly Dictionary<EndPoint, Connection> AddressPendingConnectionLookup = new Dictionary<EndPoint, Connection>();

        internal readonly Queue<NetworkEvent> UserEventQueue = new Queue<NetworkEvent>();

        private ushort pendingConnections = 0;

        private Socket ipv4Socket;
        private Socket ipv6Socket;
        private static readonly bool SupportsIPv6 = Socket.OSSupportsIPv6;

        private readonly SocketConfig config;
        private readonly NetworkSimulator simulator;
        private readonly byte[] outgoingInternalBuffer;
        private readonly byte[] incomingBuffer;

        private readonly SlidingSet<ulong> challengeInitializationVectors;

        private readonly MemoryManager memoryManager;

        public RuffleSocket(SocketConfig config)
        {
            List<string> configurationErrors = config.GetInvalidConfiguration();

            if (configurationErrors.Count > 0)
            {
                Logging.Error("Invalid configuration! Please fix the following issues [" + string.Join(",", configurationErrors.ToArray()) + "]");
            }

            this.config = config;

            if (config.UseSimulator)
            {
                simulator = new NetworkSimulator(config.SimulatorConfig, SendRawReal);
            }

            outgoingInternalBuffer = new byte[Math.Max((int)config.AmplificationPreventionHandshakePadding, 128)];
            incomingBuffer = new byte[config.MaxBufferSize];
            Connections = new Connection[config.MaxConnections];
            challengeInitializationVectors = new SlidingSet<ulong>((int)config.ConnectionChallengeHistory, true);

            memoryManager = new MemoryManager(config);

            bool bindSuccess = Bind(config.IPv4ListenAddress, config.IPv6ListenAddress, config.DualListenPort, config.UseIPv6Dual);

            if (!bindSuccess)
            {
                Logging.Error("Failed to bind socket");
            }
            else
            {
                Logging.Info("Socket was bound");
            }
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
                                catch (SocketException e)
                                {
                                    Logging.Error("Socket bind failed after setting dual mode with exception: " + e);
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

                Logging.Error("Socket bind with exception: " + bindException);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connection">The connection to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noDelay">If set to <c>true</c> the message will not be delayed or merged.</param>
        public void Send(ArraySegment<byte> payload, Connection connection, byte channelId, bool noDelay)
        {
            if (connection == null || connection.Dead)
            {
                throw new ArgumentException("Connection not alive");
            }
            else if (connection.State != ConnectionState.Connected)
            {
                throw new InvalidOperationException("Connection is not connected");
            }

            PacketHandler.SendMessage(payload, connection, channelId, noDelay, memoryManager);
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connectionId">The connectionId to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noDelay">If set to <c>true</c> the message will not be delayed or merged.</param>
        public void Send(ArraySegment<byte> payload, ulong connectionId, byte channelId, bool noDelay)
        {
            if (connectionId >= (ulong)Connections.Length || connectionId < 0)
            {
                throw new ArgumentException("ConnectionId cannot be found", nameof(connectionId));
            }

            Send(payload, Connections[connectionId], channelId, noDelay);
        }

        /// <summary>
        /// Sends an unconnected message.
        /// </summary>
        /// <param name="payload">Payload.</param>
        /// <param name="endpoint">Endpoint.</param>
        public void SendUnconnected(ArraySegment<byte> payload, IPEndPoint endpoint)
        {
            if (payload.Count > config.MaxMessageSize)
            {
                Logging.Error("Tried to send unconnected message that was too large. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                return;
            }

            // Allocate the memory
            HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 4);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.UnconnectedData, false);

            // Copy payload to borrowed memory
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 1, payload.Count);

            // Send the packet
            SendRawRealEndPoint(endpoint, payload);

            // Release memory
            memoryManager.DeAlloc(memory);
        }

        /// <summary>
        /// Starts a connection to a endpoint.
        /// </summary>
        /// <returns>The pending connection.</returns>
        /// <param name="endpoint">The endpoint to connect to.</param>
        public Connection Connect(EndPoint endpoint)
        {
            Logging.Info("Starting connect to " + endpoint);

            Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingConnection);

            if (connection != null)
            {
                // Set resend values
                connection.HandshakeResendAttempts = 1;
                connection.HandshakeLastSendTime = DateTime.Now;

                outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ConnectionRequest, false);

                if (config.TimeBasedConnectionChallenge)
                {
                    // Current unix time
                    ulong unixTimestamp = (ulong)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

                    // Save for resends
                    connection.PreConnectionChallengeTimestamp = unixTimestamp;

                    // Write the current unix time
                    for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + i] = ((byte)(unixTimestamp >> (i * 8)));

                    ulong counter = 0;
                    ulong iv = RandomProvider.GetRandomULong();

                    // Save for resends
                    connection.PreConnectionChallengeIV = iv;

                    // Find collision
                    ulong hash;
                    do
                    {
                        // Attempt to calculate a new hash collision
                        hash = HashProvider.GetStableHash64(unixTimestamp, counter, iv);

                        // Increment counter
                        counter++;
                    }
                    while ((hash << (sizeof(ulong) * 8 - config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - config.ChallengeDifficulty) != 0);

                    // Make counter 1 less
                    counter--;

                    // Save for resends
                    connection.PreConnectionChallengeCounter = counter;

                    // Write counter
                    for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + sizeof(ulong) + i] = ((byte)(counter >> (i * 8)));

                    // Write IV
                    for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + (sizeof(ulong) * 2) + i] = ((byte)(iv >> (i * 8)));
                }

                int minSize = 1 + (config.TimeBasedConnectionChallenge ? sizeof(ulong) * 3 : 0);

                Logging.Info("Sending connection request to " + endpoint);

                connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding)), true);
            }
            else
            {
                Logging.Error("Failed to allocate connection to " + endpoint);
            }

            return connection;
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// </summary>
        /// <param name="connection">The connection to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public void Disconnect(Connection connection, bool sendMessage)
        {
            if (connection == null || connection.Dead)
            {
                throw new ArgumentException("Connection not alive");
            }
            else if (connection.State != ConnectionState.Connected)
            {
                throw new InvalidOperationException("Connection is not connected");
            }

            DisconnectConnection(connection, sendMessage, false);
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// </summary>
        /// <param name="connectionId">The connectionId to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public void Disconnect(ulong connectionId, bool sendMessage)
        {
            if (connectionId >= (ulong)Connections.Length || connectionId < 0)
            {
                throw new ArgumentException("ConnectionId cannot be found", nameof(connectionId));
            }

            Disconnect(Connections[connectionId], sendMessage);
        }


        private DateTime _lastTimeoutCheckRan = DateTime.MinValue;

        /// <summary>
        /// Runs the Ruffles internals. This will check for resends, timeouts, poll the socket etc.
        /// </summary>
        public void RunInternalLoop()
        {
            InternalPollSocket();

            if (simulator != null)
            {
                simulator.RunLoop();
            }

            // Run timeout loop once every ConnectionPollDelay ms
            if ((DateTime.Now - _lastTimeoutCheckRan).TotalMilliseconds >= config.MinConnectionPollDelay)
            {
                if (config.EnablePacketMerging)
                {
                    CheckMergedPackets();
                }

                if (config.EnableTimeouts)
                {
                    CheckConnectionTimeouts();
                }

                if (config.EnableHeartbeats)
                {
                    CheckConnectionHeartbeats();
                }

                if (config.EnableConnectionRequestResends)
                {
                    CheckConnectionResends();
                }

                if (config.EnableChannelUpdates)
                {
                    RunChannelInternalUpdate();
                }

                _lastTimeoutCheckRan = DateTime.Now;
            }
        }

        private void CheckMergedPackets()
        {
            for (int i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] != null && !Connections[i].Dead)
                {
                    ArraySegment<byte>? mergedPayload = Connections[i].Merger.TryFlush();

                    if (mergedPayload != null)
                    {
                        Connections[i].SendRaw(mergedPayload.Value, true);
                    }
                }
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

                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ConnectionRequest, false);

                            if (config.TimeBasedConnectionChallenge)
                            {
                                // Write the response unix time
                                for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + x] = ((byte)(Connections[i].PreConnectionChallengeTimestamp >> (x * 8)));

                                // Write counter
                                for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + sizeof(ulong) + x] = ((byte)(Connections[i].PreConnectionChallengeCounter >> (x * 8)));

                                // Write IV
                                for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + (sizeof(ulong) * 2) + x] = ((byte)(Connections[i].PreConnectionChallengeIV >> (x * 8)));
                            }

                            int minSize = 1 + (config.TimeBasedConnectionChallenge ? sizeof(ulong) * 3 : 0);

                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding)), true);
                        }
                    }
                    else if (Connections[i].State == ConnectionState.RequestingChallenge)
                    {
                        if ((DateTime.Now - Connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeResendDelay && Connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            Connections[i].HandshakeResendAttempts++;
                            Connections[i].HandshakeLastSendTime = DateTime.Now;

                            // Write connection challenge
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeRequest, false);
                            for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + x] = ((byte)(Connections[i].ConnectionChallenge >> (x * 8)));
                            outgoingInternalBuffer[1 + sizeof(ulong)] = Connections[i].ChallengeDifficulty;
                            
                            // Send the challenge
                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1 + sizeof(ulong) + 1), true);
                        }
                    }
                    else if (Connections[i].State == ConnectionState.SolvingChallenge)
                    {
                        if ((DateTime.Now - Connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeResendDelay && Connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            Connections[i].HandshakeResendAttempts++;
                            Connections[i].HandshakeLastSendTime = DateTime.Now;

                            // Write the challenge response
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeResponse, false);
                            for (byte x = 0; x < sizeof(ulong); x++) outgoingInternalBuffer[1 + x] = ((byte)(Connections[i].ChallengeAnswer >> (x * 8)));
                            
                            // Send the challenge response
                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1 + sizeof(ulong), (int)config.AmplificationPreventionHandshakePadding)), true);
                        }
                    }
                    else if (Connections[i].State == ConnectionState.Connected)
                    {
                        if (!Connections[i].HailStatus.Completed && (DateTime.Now - Connections[i].HailStatus.LastAttempt).TotalMilliseconds > config.HandshakeResendDelay && Connections[i].HailStatus.Attempts < config.MaxHandshakeResends)
                        {
                            // Send the response
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.Hail, false);

                            // Write the amount of channels
                            outgoingInternalBuffer[1] = (byte)config.ChannelTypes.Length;

                            // Write the channel types
                            for (byte x = 0; x < (byte)config.ChannelTypes.Length; x++)
                            {
                                outgoingInternalBuffer[2 + x] = (byte)config.ChannelTypes[x];
                            }

                            Connections[i].HailStatus.Attempts++;
                            Connections[i].HailStatus.LastAttempt = DateTime.Now;

                            Connections[i].SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 2 + (byte)config.ChannelTypes.Length), true);
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
                            DisconnectConnection(Connections[i], false, true);
                        }
                    }
                    else
                    {
                        if ((DateTime.Now - Connections[i].LastMessageIn).TotalMilliseconds > config.ConnectionTimeout)
                        {
                            // This client has not answered us in way too long. Let it go
                            DisconnectConnection(Connections[i], false, true);
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
                    if ((DateTime.Now - Connections[i].LastMessageOut).TotalMilliseconds > config.HeartbeatDelay)
                    {
                        // This client has not been talked to in a long time. Send a heartbeat.

                        // Create sequenced heartbeat packet
                        HeapMemory heartbeatMemory = Connections[i].HeartbeatChannel.CreateOutgoingHeartbeatMessage();

                        // Send heartbeat
                        Connections[i].SendRaw(new ArraySegment<byte>(heartbeatMemory.Buffer, (int)heartbeatMemory.VirtualOffset, (int)heartbeatMemory.VirtualCount), false);

                        // DeAlloc the memory
                        memoryManager.DeAlloc(heartbeatMemory);
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

                HandlePacket(new ArraySegment<byte>(incomingBuffer, 0, size), _fromIPv4Endpoint);
            }

            if (ipv6Socket != null && ipv6Socket.Poll(config.MaxSocketBlockMilliseconds * 1000, SelectMode.SelectRead))
            {
                // TODO: Handle SocketException when buffer is too small.
                int size = ipv6Socket.ReceiveFrom(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ref _fromIPv6Endpoint);

                HandlePacket(new ArraySegment<byte>(incomingBuffer, 0, size), _fromIPv6Endpoint);
            }
        }

        /// <summary>
        /// Polls the RuffleSocket for incoming events about connections.
        /// </summary>
        /// <returns>The poll result.</returns>
        public NetworkEvent Poll()
        {
            if (UserEventQueue.Count > 0)
            {
                NetworkEvent @event = UserEventQueue.Dequeue();

                return @event;
            }

            return new NetworkEvent()
            {
                Connection = null,
                Socket = this,
                Data = new ArraySegment<byte>(),
                AllowUserRecycle = false,
                InternalMemory = null,
                Type = NetworkEventType.Nothing,
                ChannelId = 0,
                SocketReceiveTime = DateTime.Now,
                MemoryManager = memoryManager
            };
        }

        internal void SendRaw(Connection connection, ArraySegment<byte> payload, bool noMerge)
        {
            connection.LastMessageOut = DateTime.Now;

            if (!config.EnablePacketMerging || noMerge || !connection.Merger.TryWrite(payload))
            {
                if (simulator != null)
                {
                    simulator.Add(connection, payload);
                }
                else
                {
                    SendRawReal(connection, payload);
                }
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

        private void SendRawRealEndPoint(IPEndPoint endpoint, ArraySegment<byte> payload)
        {
            if (endpoint.AddressFamily == AddressFamily.InterNetwork)
            {
                int sent = ipv4Socket.SendTo(payload.Array, payload.Offset, payload.Count, SocketFlags.None, endpoint);
            }
            else if (endpoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                int sent = ipv6Socket.SendTo(payload.Array, payload.Offset, payload.Count, SocketFlags.None, endpoint);
            }
        }

        internal void HandlePacket(ArraySegment<byte> payload, EndPoint endpoint)
        {
            if (payload.Count < 1)
            {
                // Invalid size
                Logging.Error("Got packet of size " + payload.Count + " from " + endpoint + ". Packet is too small");
                return;
            }

            // Unpack header, dont cast to MessageType enum for safety
            HeaderPacker.Unpack(payload.Array[payload.Offset], out byte messageType, out bool fragmented);

            Logging.Info("Unpacked packet. [MessageType=" + (MessageType)messageType + "] [Fragmented=" + fragmented + "]");

            switch (messageType)
            {
                case (byte)MessageType.Merge:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            if (!config.EnablePacketMerging)
                            {
                                // Big missmatch here.
                                DisconnectConnection(connection, false, false);
                                return;
                            }

                            // Unpack the merged packet
                            List<ArraySegment<byte>> segments = connection.Merger.Unpack(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1));

                            if (segments != null)
                            {
                                for (int i = 0; i < segments.Count; i++)
                                {
                                    // Handle the segment
                                    HandlePacket(segments[i], endpoint);
                                }
                            }
                        }
                    }
                    break;
                case (byte)MessageType.ConnectionRequest:
                    {
                        if (payload.Count < config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification. 
                            return;
                        }

                        if (config.TimeBasedConnectionChallenge)
                        {
                            // Get the current unix time seconds
                            ulong currentUnixTime = (ulong)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;

                            // Read the time they used
                            ulong challengeUnixTime = (((ulong)payload.Array[payload.Offset + 1]) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 1] << 8) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 2] << 16) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 3] << 24) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 4] << 32) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 5] << 40) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 6] << 48) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 7] << 56));

                            // The seconds diff
                            long secondsDiff = (long)currentUnixTime - (long)challengeUnixTime;

                            if (secondsDiff > (long)config.ConnectionChallengeTimeWindow || secondsDiff < -(long)config.ConnectionChallengeTimeWindow)
                            {
                                // Outside the allowed window
                                Logging.Error("Client " + endpoint + " failed the connection request. They were outside of their allowed window. The diff was " + Math.Abs(secondsDiff) + " seconds");
                                return;
                            }

                            // Read the counter they used to collide the hash
                            ulong counter = (((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong)]) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 1] << 8) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 2] << 16) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 3] << 24) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 4] << 32) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 5] << 40) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 6] << 48) |
                                            ((ulong)payload.Array[payload.Offset + 1 + sizeof(ulong) + 7] << 56));

                            // Read the initialization vector they used
                            ulong userIv = (((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2)]) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 1] << 8) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 2] << 16) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 3] << 24) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 4] << 32) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 5] << 40) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 6] << 48) |
                                            ((ulong)payload.Array[payload.Offset + 1 + (sizeof(ulong) * 2) + 7] << 56));

                            // Ensure they dont reuse a IV
                            if (challengeInitializationVectors[userIv])
                            {
                                // This IV is being reused.
                                Logging.Error("Client " + endpoint + " failed the connection request. They were trying to reuse an IV");
                                return;
                            }

                            // Calculate the hash the user claims have a collision
                            ulong claimedHash = HashProvider.GetStableHash64(challengeUnixTime, counter, userIv);

                            // Check if the hash collides
                            bool isCollided = ((claimedHash << (sizeof(ulong) * 8 - config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - config.ChallengeDifficulty)) == 0;

                            if (!isCollided)
                            {
                                // They failed the challenge
                                Logging.Error("Client " + endpoint + " failed the connection request. They submitted an invalid answer");
                                return;
                            }

                            // Save the IV to the sliding window
                            challengeInitializationVectors[userIv] = true;
                        }

                        Logging.Info("Client " + endpoint + " is being challenged");

                        Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingChallenge);

                        if (connection != null)
                        {
                            // This connection was successfully added as pending

                            // Set resend values
                            connection.HandshakeResendAttempts = 1;
                            connection.HandshakeLastSendTime = DateTime.Now;

                            // Write connection challenge
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeRequest, false);
                            for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + i] = ((byte)(connection.ConnectionChallenge >> (i * 8)));
                            outgoingInternalBuffer[1 + sizeof(ulong)] = connection.ChallengeDifficulty;

                            // Send the challenge
                            connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1 + sizeof(ulong) + 1), true);

                            Logging.Info("Client " + endpoint + " was sent a challenge of difficulty " + connection.ChallengeDifficulty);
                        }
                        else
                        {
                            Logging.Error("Client " + endpoint + " could not be challenged. Allocation failed");
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeRequest:
                    {
                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingConnection)
                        {
                            if (payload.Count < 10)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                Logging.Error("Server " + endpoint + " sent us a payload that was too small. Disconnecting");
                                DisconnectConnection(connection, false, false);
                                return;
                            }

                            connection.LastMessageIn = DateTime.Now;

                            connection.ConnectionChallenge = (((ulong)payload.Array[payload.Offset + 1 + 0]) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 1] << 8) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 2] << 16) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 3] << 24) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 4] << 32) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 5] << 40) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 6] << 48) |
                                                                ((ulong)payload.Array[payload.Offset + 1 + 7] << 56));

                            connection.ChallengeDifficulty = payload.Array[payload.Offset + 1 + sizeof(ulong)];

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
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeResponse, false);
                            for (byte i = 0; i < sizeof(ulong); i++) outgoingInternalBuffer[1 + i] = ((byte)(additionsRequired >> (i * 8)));

                            // Send the challenge response
                            connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, Math.Max(1 + sizeof(ulong), (int)config.AmplificationPreventionHandshakePadding)), true);

                            Logging.Info("Server " + endpoint + " challenge of difficulty " + connection.ChallengeDifficulty + " was solved. Answer was sent");
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeResponse:
                    {
                        if (payload.Count < config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification
                            Logging.Error("Client " + endpoint + " sent a challenge response that was smaller than the amplification padding");
                            return;
                        }

                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingChallenge)
                        {
                            if (payload.Count < 9)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                DisconnectConnection(connection, false, false);
                                return;
                            }

                            connection.LastMessageIn = DateTime.Now;

                            ulong challengeResponse = (((ulong)payload.Array[payload.Offset + 1 + 0]) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 1] << 8) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 2] << 16) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 3] << 24) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 4] << 32) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 5] << 40) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 6] << 48) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 7] << 56));

                            ulong claimedCollision = connection.ConnectionChallenge + challengeResponse;

                            bool isCollided = connection.ChallengeDifficulty == 0 || ((HashProvider.GetStableHash64(claimedCollision) << ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) >> ((sizeof(ulong) * 8) - connection.ChallengeDifficulty)) == 0;

                            if (isCollided)
                            {
                                // Success, they completed the hashcash challenge

                                Logging.Info("Client " + endpoint + " successfully completed challenge of difficulty " + connection.ChallengeDifficulty);

                                ConnectPendingConnection(connection);

                                Logging.Info("Client " + endpoint + " state changed to connected");

                                connection.HailStatus.Attempts = 1;
                                connection.HailStatus.HasAcked = false;
                                connection.HailStatus.LastAttempt = DateTime.Now;

                                // Send the response
                                outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.Hail, false);

                                // Write the amount of channels
                                outgoingInternalBuffer[1] = (byte)config.ChannelTypes.Length;

                                // Write the channel types
                                for (byte i = 0; i < (byte)config.ChannelTypes.Length; i++)
                                {
                                    outgoingInternalBuffer[2 + i] = (byte)config.ChannelTypes[i];
                                }

                                connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 2 + (byte)config.ChannelTypes.Length), true);

                                Logging.Info("Client " + endpoint + " was sent a hail");

                                // Send to userspace
                                UserEventQueue.Enqueue(new NetworkEvent()
                                {
                                    Connection = connection,
                                    Socket = this,
                                    Type = NetworkEventType.Connect,
                                    AllowUserRecycle = false,
                                    ChannelId = 0,
                                    Data = new ArraySegment<byte>(),
                                    InternalMemory = null,
                                    SocketReceiveTime = DateTime.Now,
                                    MemoryManager = memoryManager
                                });
                            }
                            else
                            {
                                // Failed, disconnect them
                                Logging.Error("Client " + endpoint + " failed the challenge. Disconnecting");

                                DisconnectConnection(connection, false, false);
                            }
                        }
                        else
                        {
                            Logging.Warning("Client " + endpoint + " sent a challenge response but they were either not connected or were not in a RequestingChallenge state. Delayed packets?");
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
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.HailConfirmed, false);

                            // Send confirmation
                            connectedConnection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1), true);

                            Logging.Info("Hail confirmation sent to " + endpoint);
                        }
                        else if (pendingConnection != null && pendingConnection.State == ConnectionState.SolvingChallenge)
                        {
                            if (payload.Count < 2)
                            {
                                // Invalid size.
                                Logging.Error("Client " + endpoint + " sent a payload that was too small. Disconnecting");

                                DisconnectConnection(pendingConnection, false, false);
                                return;
                            }

                            pendingConnection.LastMessageIn = DateTime.Now;

                            // Read the amount of channels
                            byte channelCount = payload.Array[payload.Offset + 1];

                            if (payload.Count < channelCount + 2)
                            {
                                // Invalid size.
                                Logging.Error("Client " + endpoint + " sent a payload that was too small. Disconnecting");
                                DisconnectConnection(pendingConnection, false, false);
                                return;
                            }

                            // Alloc the types
                            pendingConnection.ChannelTypes = new ChannelType[channelCount];

                            // Read the types
                            for (byte i = 0; i < channelCount; i++)
                            {
                                byte channelType = payload.Array[payload.Offset + 2 + i];

                                switch (channelType)
                                {
                                    case (byte)ChannelType.Reliable:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.Reliable;
                                        }
                                        break;
                                    case (byte)ChannelType.Unreliable:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.Unreliable;
                                        }
                                        break;
                                    case (byte)ChannelType.UnreliableSequenced:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.UnreliableSequenced;
                                        }
                                        break;
                                    case (byte)ChannelType.ReliableSequenced:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.ReliableSequenced;
                                        }
                                        break;
                                    case (byte)ChannelType.UnreliableRaw:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.UnreliableRaw;
                                        }
                                        break;
                                    case (byte)ChannelType.ReliableSequencedFragmented:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.ReliableSequencedFragmented;
                                        }
                                        break;
                                    default:
                                        {
                                            // Unknown channel type. Disconnect.
                                            Logging.Error("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                            DisconnectConnection(pendingConnection, false, false);
                                            return;
                                        }
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
                            for (byte i = 0; i < pendingConnection.ChannelTypes.Length; i++)
                            {
                                switch (pendingConnection.ChannelTypes[i])
                                {
                                    case ChannelType.Reliable:
                                        {
                                            pendingConnection.Channels[i] = new ReliableChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    case ChannelType.Unreliable:
                                        {
                                            pendingConnection.Channels[i] = new UnreliableChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    case ChannelType.UnreliableSequenced:
                                        {
                                            pendingConnection.Channels[i] = new UnreliableSequencedChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    case ChannelType.ReliableSequenced:
                                        {
                                            pendingConnection.Channels[i] = new ReliableSequencedChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    case ChannelType.UnreliableRaw:
                                        {
                                            pendingConnection.Channels[i] = new UnreliableRawChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    case ChannelType.ReliableSequencedFragmented:
                                        {
                                            pendingConnection.Channels[i] = new ReliableSequencedFragmentedChannel(i, pendingConnection, config, memoryManager);
                                        }
                                        break;
                                    default:
                                        {
                                            // Unknown channel type. Disconnect.
                                            Logging.Error("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                            DisconnectConnection(pendingConnection, false, false);
                                            return;
                                        }
                                }
                            }

                            // Set state to connected
                            ConnectPendingConnection(pendingConnection);

                            Logging.Info("Client " + endpoint + " state changed to connected");

                            // Send to userspace
                            UserEventQueue.Enqueue(new NetworkEvent()
                            {
                                Connection = pendingConnection,
                                Socket = this,
                                Type = NetworkEventType.Connect,
                                AllowUserRecycle = false,
                                ChannelId = 0,
                                Data = new ArraySegment<byte>(),
                                InternalMemory = null,
                                SocketReceiveTime = DateTime.Now,
                                MemoryManager = memoryManager
                            });

                            // Send the confirmation
                            outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.HailConfirmed, false);

                            // Send confirmation
                            pendingConnection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1), true);

                            Logging.Info("Client " + endpoint + " was sent hail confimrations");
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
                        else
                        {
                            Logging.Error("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Heartbeat:
                    {
                        if (!config.EnableHeartbeats)
                        {
                            // TODO: Handle
                            // This is a missmatch.
                            Logging.Error("Heartbeat received from " + endpoint + " but the we do not have heartbeats enabled. Configuration missmatch?");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            // Heartbeats are sequenced to not properly handle network congestion

                            if (connection.HeartbeatChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), out bool hasMore) != null)
                            {
                                connection.LastMessageIn = DateTime.Now;
                            }
                        }
                        else
                        {
                            Logging.Error("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Data:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.LastMessageIn = DateTime.Now;

                            PacketHandler.HandleIncomingMessage(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), connection, config, memoryManager);
                        }
                        else
                        {
                            Logging.Error("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Ack:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.LastMessageIn = DateTime.Now;

                            byte channelId = payload.Array[payload.Offset + 1];

                            if (channelId < 0 || channelId >= connection.Channels.Length)
                            {
                                // Invalid channelId
                                return;
                            }

                            IChannel channel = connection.Channels[channelId];

                            // Handle ack
                            channel.HandleAck(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Count - 2));
                        }
                        else
                        {
                            Logging.Error("Client " + endpoint + " connection could not be found");
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
                        else
                        {
                            Logging.Error("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.UnconnectedData:
                    {
                        if (config.AllowUnconnectedMessages)
                        {
                            // Alloc memory that can be borrowed to userspace
                            HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count - 1);

                            // Copy payload to borrowed memory
                            Buffer.BlockCopy(payload.Array, payload.Offset + 1, memory.Buffer, 0, payload.Count - 1);

                            // Send to userspace
                            UserEventQueue.Enqueue(new NetworkEvent()
                            {
                                Connection = null,
                                Socket = this,
                                Type = NetworkEventType.UnconnectedData,
                                AllowUserRecycle = true,
                                Data = new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount),
                                InternalMemory = memory,
                                SocketReceiveTime = DateTime.Now,
                                ChannelId = 0,
                                MemoryManager = memoryManager
                            });
                        }
                        else
                        {
                            Logging.Warning("Got unconnected message but SocketConfig.AllowUnconnectedMessages is disabled.");
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

            pendingConnections++;
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

        internal void DisconnectConnection(Connection connection, bool sendMessage, bool timeout)
        {
            if (connection.State == ConnectionState.Connected && sendMessage && !timeout)
            {
                // Send disconnect message

                // Write disconnect header
                outgoingInternalBuffer[0] = HeaderPacker.Pack((byte)MessageType.Disconnect, false);

                // Send disconnect message
                connection.SendRaw(new ArraySegment<byte>(outgoingInternalBuffer, 0, 1), true);
            }

            if (config.ReuseConnections)
            {
                // Mark as dead, this will allow it to be reclaimed
                connection.Dead = true;

                // Reset all channels, releasing memory etc
                for (int i = 0; i < connection.Channels.Length; i++)
                {
                    connection.Channels[i].Reset();
                }

                if (config.EnableHeartbeats)
                {
                    // Release all memory from the heartbeat channel
                    connection.HeartbeatChannel.Reset();
                }

                if (config.EnablePacketMerging)
                {
                    // Clean the merger
                    connection.Merger.Clear();
                }
            }
            else
            {
                // Release to GC unless user has a hold of it
                Connections[connection.Id] = null;
            }

            // Remove connection lookups
            if (connection.State != ConnectionState.Connected)
            {
                AddressPendingConnectionLookup.Remove(connection.EndPoint);

                pendingConnections--;
            }
            else
            {
                AddressConnectionLookup.Remove(connection.EndPoint);
            }

            // Send disconnect to userspace
            UserEventQueue.Enqueue(new NetworkEvent()
            {
                Connection = connection,
                Socket = this,
                Type = timeout ? NetworkEventType.Timeout : NetworkEventType.Disconnect,
                AllowUserRecycle = false,
                ChannelId = 0,
                Data = new ArraySegment<byte>(),
                InternalMemory = null,
                SocketReceiveTime = DateTime.Now,
                MemoryManager = memoryManager
            });
        }

        internal Connection AddNewConnection(EndPoint endpoint, ConnectionState state)
        {
            // Make sure they are not already connected to prevent an attack where a single person can fill all the slots.
            if (AddressPendingConnectionLookup.ContainsKey(endpoint) || AddressConnectionLookup.ContainsKey(endpoint) || pendingConnections > config.MaxPendingConnections)
            {
                return null;
            }

            Connection connection = null;

            for (ushort i = 0; i < Connections.Length; i++)
            {
                if (Connections[i] == null)
                {
                    // Alloc on the heap
                    connection = new Connection(config, memoryManager)
                    {
                        Dead = false,
                        Recycled = false,
                        Id = i,
                        State = state,
                        HailStatus = new MessageStatus(),
                        Socket = this,
                        EndPoint = endpoint,
                        ConnectionChallenge = RandomProvider.GetRandomULong(),
                        ChallengeDifficulty = config.ChallengeDifficulty,
                        LastMessageIn = DateTime.Now,
                        LastMessageOut = DateTime.Now,
                        ConnectionStarted = DateTime.Now,
                        HandshakeResendAttempts = 0,
                        ChallengeAnswer = 0,
                        Channels = new IChannel[0],
                        ChannelTypes = new ChannelType[0],
                        HandshakeLastSendTime = DateTime.Now,
                        Roundtrip = 10,
                        Merger = config.EnablePacketMerging ? new MessageMerger(config.MaxMergeMessageSize, config.MaxMergeDelay) : null
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
                                    connection.Channels[x] = new ReliableChannel(x, connection, config, memoryManager);
                                }
                                break;
                            case ChannelType.Unreliable:
                                {
                                    connection.Channels[x] = new UnreliableChannel(x, connection, config, memoryManager);
                                }
                                break;
                            case ChannelType.UnreliableSequenced:
                                {
                                    connection.Channels[x] = new UnreliableSequencedChannel(x, connection, config, memoryManager);
                                }
                                break;
                            case ChannelType.ReliableSequenced:
                                {
                                    connection.Channels[x] = new ReliableSequencedChannel(x, connection, config, memoryManager);
                                }
                                break;
                            case ChannelType.UnreliableRaw:
                                {
                                    connection.Channels[x] = new UnreliableRawChannel(x, connection, config, memoryManager);
                                }
                                break;
                            case ChannelType.ReliableSequencedFragmented:
                                {
                                    connection.Channels[x] = new ReliableSequencedFragmentedChannel(x, connection, config, memoryManager);
                                }
                                break;
                            default:
                                {
                                    // Unknown channel type. Disconnect.
                                    // TODO: Fix
                                    Logging.Error("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                    DisconnectConnection(connection, false, false);
                                }
                                break;
                        }
                    }

                    Connections[i] = connection;
                    AddressPendingConnectionLookup.Add(endpoint, connection);

                    pendingConnections++;

                    break;
                }
                else if (Connections[i].Dead && Connections[i].Recycled)
                {
                    // This is no longer used, reuse it
                    connection = Connections[i];
                    connection.Dead = false;
                    connection.Recycled = false;
                    connection.State = state;
                    connection.HailStatus = new MessageStatus();
                    connection.Id = i;
                    connection.Socket = this;
                    connection.EndPoint = endpoint;
                    connection.ConnectionChallenge = RandomProvider.GetRandomULong();
                    connection.ChallengeDifficulty = config.ChallengeDifficulty;
                    connection.LastMessageOut = DateTime.Now;
                    connection.LastMessageIn = DateTime.Now;
                    connection.ConnectionStarted = DateTime.Now;
                    connection.ChallengeAnswer = 0;
                    connection.HandshakeLastSendTime = DateTime.Now;
                    connection.Roundtrip = 10;

                    AddressPendingConnectionLookup.Add(endpoint, connection);

                    pendingConnections++;

                    break;
                }
            }

            return connection;
        }
    }
}
