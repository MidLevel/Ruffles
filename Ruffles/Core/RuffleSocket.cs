using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Ruffles.Channeling;
using Ruffles.Channeling.Channels;
using Ruffles.Collections;
using Ruffles.Configuration;
using Ruffles.Connections;
using Ruffles.Hashing;
using Ruffles.Memory;
using Ruffles.Messaging;
using Ruffles.Random;
using Ruffles.Simulation;
using Ruffles.Time;
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
        /// <summary>
        /// Event that is raised when a network event occurs. This can be used as an alternative, or in combination with polling.
        /// </summary>
        public event Action<NetworkEvent> OnNetworkEvent;

        internal void PublishEvent(NetworkEvent @event)
        {
            bool delivered = false;

            if (config.EnablePollEvents)
            {
                delivered = true;
                userEventQueue.Enqueue(@event);
            }

            if (config.EnableCallbackEvents && OnNetworkEvent != null)
            {
                delivered = true;
                OnNetworkEvent(@event);
            }

            if (!delivered)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Unable to deliver event. Make sure you have PollEvents enabled and/or CallbackEvents enabled with a registered handler");
            }
        }

        // Separate connections and pending to prevent something like a slorris attack
        private readonly Connection[] connections;
        private readonly Dictionary<EndPoint, Connection> addressConnectionLookup = new Dictionary<EndPoint, Connection>();
        private readonly Dictionary<EndPoint, Connection> addressPendingConnectionLookup = new Dictionary<EndPoint, Connection>();

        // Lock for adding or removing connections. This is done to allow for a quick ref to be gained on the user thread when connecting.
        private readonly object connectionAddRemoveLock = new object();

        // TODO: Removed hardcoded size
        private readonly ConcurrentCircularQueue<NetworkEvent> userEventQueue;
        private readonly ConcurrentCircularQueue<InternalEvent> internalEventQueue;

        private ushort pendingConnections = 0;

        private Socket ipv4Socket;
        private Socket ipv6Socket;
        private static readonly bool SupportsIPv6 = Socket.OSSupportsIPv6;

        private readonly SocketConfig config;
        private readonly NetworkSimulator simulator;
        private readonly byte[] incomingBuffer;

        private readonly SlidingSet<ulong> challengeInitializationVectors;

        private readonly MemoryManager memoryManager;
        private readonly ChannelPool channelPool;

        private Thread _networkThread;

        private bool _initialized;

        public bool IsRunning { get; private set; }
        public bool IsTerminated { get; private set; }

        /// <summary>
        /// Gets the local IPv4 listening endpoint.
        /// </summary>
        /// <value>The local IPv4 endpoint.</value>
        public EndPoint LocalIPv4EndPoint
        {
            get
            {
                if (ipv4Socket == null)
                {
                    return new IPEndPoint(IPAddress.Any, 0);
                }

                return ipv4Socket.LocalEndPoint;
            }
        }

        /// <summary>
        /// Gets the local IPv6 listening endpoint.
        /// </summary>
        /// <value>The local IPv6 endpoint.</value>
        public EndPoint LocalIPv6EndPoint
        {
            get
            {
                if (ipv6Socket == null)
                {
                    return new IPEndPoint(IPAddress.IPv6Any, 0);
                }

                return ipv6Socket.LocalEndPoint;
            }
        }

        public RuffleSocket(SocketConfig config)
        {
            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Checking SokcetConfig validity...");

            List<string> configurationErrors = config.GetInvalidConfiguration();

            if (configurationErrors.Count > 0)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Invalid configuration! Please fix the following issues [" + string.Join(",", configurationErrors.ToArray()) + "]");
            }
            else
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("SocketConfig is valid");
            }

            this.config = config;

            if (config.UseSimulator)
            {
                simulator = new NetworkSimulator(config.SimulatorConfig, SendRawReal);
                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Simulator ENABLED");
            }
            else
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Simulator DISABLED");
            }

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.EventQueueSize + " event slots");
            userEventQueue = new ConcurrentCircularQueue<NetworkEvent>(config.EventQueueSize);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.InternalEventQueueSize + " internal event slots");
            internalEventQueue = new ConcurrentCircularQueue<InternalEvent>(config.InternalEventQueueSize);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.MaxBufferSize + " bytes of incomingBuffer");
            incomingBuffer = new byte[config.MaxBufferSize];

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.MaxConnections + " connection slots");
            connections = new Connection[config.MaxConnections];

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.ConnectionChallengeHistory + " challenge IV slots");
            challengeInitializationVectors = new SlidingSet<ulong>((int)config.ConnectionChallengeHistory, true);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating memory manager");
            memoryManager = new MemoryManager(config);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating channel pool");
            channelPool = new ChannelPool(config);

            if (!NetTime.HighResolution)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("NetTime does not support high resolution. This might impact Ruffles performance");
            }
        }

        /// <summary>
        /// Starts the socket.
        /// </summary>
        public bool Start()
        {
            if (IsRunning)
            {
                throw new InvalidOperationException("Socket already started");
            }

            if (!_initialized)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Binding socket");
                bool bindSuccess = Bind(config.IPv4ListenAddress, config.IPv6ListenAddress, config.DualListenPort, config.UseIPv6Dual);

                if (!bindSuccess)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Failed to bind socket");
                    return false;
                }
                else
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Socket was successfully bound");
                    _initialized = true;
                }
            }

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Starting networking thread");

            // Create network thread
            _networkThread = new Thread(StartNetworkLogic)
            {
                Name = "NetworkThread",
                IsBackground = true
            };

            // Set running state to true
            IsRunning = true;

            // Start network thread
            _networkThread.Start();

            return true;
        }

        /// <summary>
        /// Stops the socket.
        /// </summary>
        public void Stop()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Cannot stop a non running socket");
            }

            // Disconnect all clients
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead && connections[i].State != ConnectionState.Disconnected)
                {
                    connections[i].Disconnect(true);
                }
            }

            IsRunning = false;
            _networkThread.Join();
        }

        /// <summary>
        /// Shuts the socket down.
        /// </summary>
        public void Shutdown()
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("Cannot shutdown a non initialized socket");
            }

            IsTerminated = true;
            _initialized = false;

            if (IsRunning)
            {
                Stop();
            }

            ipv4Socket.Close();
            ipv6Socket.Close();
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

            // Set the .NET buffer sizes. Defaults to 1 megabyte each
            socket.ReceiveBufferSize = Constants.RECEIVE_SOCKET_BUFFER_SIZE;
            socket.SendBufferSize = Constants.SEND_SOCKET_BUFFER_SIZE;

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
                                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Socket bind failed after setting dual mode with exception: " + e);
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

                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Socket bind with exception: " + bindException);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// This will send the packet straight away. 
        /// This can cause the channel to lock up. 
        /// For higher performance sends, use SendLater.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connection">The connection to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noMerge">If set to <c>true</c> the message will not be merged.</param>
        public bool SendNow(ArraySegment<byte> payload, Connection connection, byte channelId, bool noMerge)
        {
            if (connection != null && !connection.Dead && connection.State == ConnectionState.Connected)
            {
                PacketHandler.SendMessage(payload, connection, channelId, noMerge, memoryManager);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// This will send the packet straight away. 
        /// This can cause the channel to lock up. 
        /// For higher performance sends, use SendLater.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connectionId">The connectionId to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noMerge">If set to <c>true</c> the message will not be merged.</param>
        public bool SendNow(ArraySegment<byte> payload, ulong connectionId, byte channelId, bool noMerge)
        {
            if (connectionId < (ulong)connections.Length && connectionId >= 0)
            {
                return SendNow(payload, connections[connectionId], channelId, noMerge);
            }

            return false;
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// This will send the packet on the next network IO tick. 
        /// This adds additional send delay but prevents the channel from locking. 
        /// For reduced delay, use SendNow.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connection">The connection to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noMerge">If set to <c>true</c> the message will not be merged.</param>
        public bool SendLater(ArraySegment<byte> payload, Connection connection, byte channelId, bool noMerge)
        {
            if (!config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call SendLater when EnableQueuedIOEvents is disabled");
            }

            if (connection != null && !connection.Dead && connection.State == ConnectionState.Connected)
            {
                // Alloc memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count);

                // Copy the memory
                Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 0, payload.Count);

                internalEventQueue.Enqueue(new InternalEvent()
                {
                    Type = InternalEvent.InternalEventType.Send,
                    Connection = connection,
                    ChannelId = channelId,
                    NoMerge = noMerge,
                    Data = memory
                });

                return true;
            }

            return false;
        }

        /// <summary>
        /// Sends the specified payload to a connection.
        /// This will send the packet on the next network IO tick. 
        /// This adds additional send delay but prevents the channel from locking. 
        /// For reduced delay, use SendNow.
        /// </summary>
        /// <param name="payload">The payload to send.</param>
        /// <param name="connectionId">The connectionId to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noMerge">If set to <c>true</c> the message will not be merged.</param>
        public bool SendLater(ArraySegment<byte> payload, ulong connectionId, byte channelId, bool noMerge)
        {
            if (connectionId < (ulong)connections.Length && connectionId >= 0)
            {
                return SendLater(payload, connections[connectionId], channelId, noMerge);
            }

            return false;
        }

        /// <summary>
        /// Sends an unconnected message.
        /// </summary>
        /// <param name="payload">Payload.</param>
        /// <param name="endpoint">Endpoint.</param>
        public void SendUnconnected(ArraySegment<byte> payload, IPEndPoint endpoint)
        {
            if (payload.Count > config.MinimumMTU)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error)  Logging.LogError("Tried to send unconnected message that was too large. [Size=" + payload.Count + "] [MaxMessageSize=" + config.MaxFragments + "]");
                return;
            }

            // Allocate the memory
            HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count + 1);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.UnconnectedData, false);

            // Copy payload to borrowed memory
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 1, payload.Count);

            // Send the packet
            SendRawRealEndPoint(endpoint, new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount));

            // Release memory
            memoryManager.DeAlloc(memory);
        }

        /// <summary>
        /// Starts a connection to a endpoint.
        /// This does the connection security logic on the calling thread. NOT the network thread. 
        /// If you have a high security connection, the solver will run on the caller thread.
        /// Note that this call will block the network thread and will cause slowdowns. Use ConnectLater for non blocking IO
        /// </summary>
        /// <returns>The pending connection.</returns>
        /// <param name="endpoint">The endpoint to connect to.</param>
        public Connection ConnectNow(EndPoint endpoint)
        {
            if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Attempting to connect (now) to " + endpoint);

            ulong unixTimestamp = 0;
            ulong iv = 0;
            ulong counter = 0;

            if (config.TimeBasedConnectionChallenge)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Calculating with difficulty " + config.ChallengeDifficulty);

                // Current unix time
                unixTimestamp = (ulong)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

                // Generate IV
                iv = RandomProvider.GetRandomULong();

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

                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Found hash collision after " + counter + " attempts. [Counter=" + (counter - 1) + "] [IV=" + iv + "] [Time=" + unixTimestamp + "] [Hash=" + hash + "]");

                // Make counter 1 less
                counter--;
            }

            return ConnectInternal(endpoint, unixTimestamp, counter, iv);
        }

        /// <summary>
        /// Starts a connection to a endpoint.
        /// This does the connection security logic on the calling thread. NOT the network thread. 
        /// If you have a high security connection, the solver will run on the caller thread.
        /// This call will NOT block the network thread and is faster than ConnectNow.
        /// </summary>
        /// <param name="endpoint">The endpoint to connect to.</param>
        public void ConnectLater(EndPoint endpoint)
        {
            if (!config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call ConnectLater when EnableQueuedIOEvents is disabled");
            }

            if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Attempting to connect (later) to " + endpoint);

            InternalEvent @event = new InternalEvent()
            {
                Type = InternalEvent.InternalEventType.Connect,
                Endpoint = endpoint
            };

            if (config.TimeBasedConnectionChallenge)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Calculating with difficulty " + config.ChallengeDifficulty);

                // Current unix time
                ulong unixTimestamp = (ulong)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

                // Save for resends
                @event.PreConnectionChallengeTimestamp = unixTimestamp;

                ulong counter = 0;
                ulong iv = RandomProvider.GetRandomULong();

                // Save for resends
                @event.PreConnectionChallengeIV = iv;

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

                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Found hash collision after " + counter + " attempts. [Counter=" + (counter - 1) + "] [IV=" + iv + "] [Time=" + unixTimestamp + "] [Hash=" + hash + "]");

                // Make counter 1 less
                counter--;

                // Save for resends
                @event.PreConnectionChallengeCounter = counter;
            }

            // Queue the event for the network thread
            internalEventQueue.Enqueue(@event);
        }

        private Connection ConnectInternal(EndPoint endpoint, ulong unixTimestamp, ulong counter, ulong iv)
        {
            Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingConnection);

            if (connection != null)
            {
                // Set resend values
                connection.HandshakeResendAttempts = 1;
                connection.HandshakeLastSendTime = NetTime.Now;

                // Calculate the minimum size we could use for a packet
                int minSize = 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (config.TimeBasedConnectionChallenge ? sizeof(ulong) * 3 : 0);

                // Calculate the actual size with respect to amplification padding
                int size = Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding);

                // Allocate the memory
                HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                // Set the header
                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ConnectionRequest, false);

                // Copy the identification token
                Buffer.BlockCopy(Constants.RUFFLES_PROTOCOL_IDENTIFICATION, 0, memory.Buffer, 1, Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length);

                if (config.TimeBasedConnectionChallenge)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Writing solve with difficulty " + config.ChallengeDifficulty);

                    // Save for resends
                    connection.PreConnectionChallengeTimestamp = unixTimestamp;

                    // Write the current unix time
                    for (byte i = 0; i < sizeof(ulong); i++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + i] = ((byte)(unixTimestamp >> (i * 8)));

                    // Save for resends
                    connection.PreConnectionChallengeCounter = counter;

                    // Write counter
                    for (byte i = 0; i < sizeof(ulong); i++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + i] = ((byte)(counter >> (i * 8)));

                    // Save for resends
                    connection.PreConnectionChallengeIV = iv;

                    // Write IV
                    for (byte i = 0; i < sizeof(ulong); i++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + i] = ((byte)(iv >> (i * 8)));

                    // Mark it as solved (for resending)
                    connection.PreConnectionChallengeSolved = true;
                }

                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Sending connection request to " + endpoint);

                // Send the packet
                connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                // Dealloc the memory
                memoryManager.DeAlloc(memory);
            }
            else
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Failed to allocate connection to " + endpoint);
            }

            return connection;
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// This call will NOT block the network thread and is faster than DisconnectNow.
        /// </summary>
        /// <param name="connection">The connection to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public bool DisconnectLater(Connection connection, bool sendMessage)
        {
            if (!config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call DisconnectLater when EnableQueuedIOEvents is disabled");
            }

            if (connection != null && !connection.Dead && connection.State == ConnectionState.Connected)
            {
                // Queue the event for the network thread
                internalEventQueue.Enqueue(new InternalEvent()
                {
                    Type = InternalEvent.InternalEventType.Disconnect,
                    Connection = connection,
                    SendMessage = sendMessage
                });

                return true;
            }

            return false;
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// This call will NOT block the network thread and is faster than DisconnectNow.
        /// </summary>
        /// <param name="connectionId">The connectionId to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public bool DisconnectLater(ulong connectionId, bool sendMessage)
        {
            if (connectionId < (ulong)connections.Length && connectionId >= 0)
            {
                return DisconnectLater(connections[connectionId], sendMessage);
            }

            return false;
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// </summary>
        /// <param name="connection">The connection to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public bool DisconnectNow(Connection connection, bool sendMessage)
        {
            if (connection != null && !connection.Dead && connection.State == ConnectionState.Connected)
            {
                DisconnectInternal(connection, sendMessage, false);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Disconnect the specified connection.
        /// </summary>
        /// <param name="connectionId">The connectionId to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public bool DisconnectNow(ulong connectionId, bool sendMessage)
        {
            if (connectionId < (ulong)connections.Length && connectionId >= 0)
            {
                return DisconnectNow(connections[connectionId], sendMessage);
            }

            return false;
        }

        private void StartNetworkLogic()
        {
            while (IsRunning)
            {
                try
                {
                    InternalPollSocket(config.SocketPollTime);

                    if (simulator != null)
                    {
                        simulator.RunLoop();
                    }

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

                    if (config.EnablePathMTU)
                    {
                        RunPathMTU();
                    }

                    if (config.EnableQueuedIOEvents)
                    {
                        PollInternalIOQueue();
                    }
                }
                catch (Exception e)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when running internal loop: " + e);
                }
            }
        }

        private void CheckMergedPackets()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead)
                {
                    ArraySegment<byte>? mergedPayload = connections[i].Merger.TryFlush(out ushort headerSize);

                    if (mergedPayload != null)
                    {
                        connections[i].SendRaw(mergedPayload.Value, true, headerSize);
                    }
                }
            }
        }

        private void RunPathMTU()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead && connections[i].State == ConnectionState.Connected && 
                    connections[i].MTU < config.MaximumMTU && connections[i].MTUStatus.Attempts < config.MaxMTUAttempts && (NetTime.Now - connections[i].MTUStatus.LastAttempt).TotalMilliseconds > config.MTUAttemptDelay)
                {
                    uint attemptedMtu = (uint)(connections[i].MTU * config.MTUGrowthFactor);

                    if (attemptedMtu > config.MaximumMTU)
                    {
                        attemptedMtu = config.MaximumMTU;
                    }

                    if (attemptedMtu < config.MinimumMTU)
                    {
                        attemptedMtu = config.MinimumMTU;
                    }

                    connections[i].MTUStatus.Attempts++;
                    connections[i].MTUStatus.LastAttempt = NetTime.Now;

                    // Allocate memory
                    HeapMemory memory = memoryManager.AllocHeapMemory((uint)attemptedMtu);

                    // Set the header
                    memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.MTURequest, false);

                    // Send the request
                    connections[i].SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                    // Dealloc the memory
                    memoryManager.DeAlloc(memory);
                }
            }
        }

        private void RunChannelInternalUpdate()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead && connections[i].Channels != null)
                {
                    for (int x = 0; x < connections[i].Channels.Length; x++)
                    {
                        if (connections[i].Channels[x] != null)
                        {
                            connections[i].Channels[x].InternalUpdate();
                        }
                    }
                }
            }
        }

        private void CheckConnectionResends()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead)
                {
                    if (connections[i].State == ConnectionState.RequestingConnection)
                    {
                        if ((!config.TimeBasedConnectionChallenge || connections[i].PreConnectionChallengeSolved) && (NetTime.Now - connections[i].HandshakeLastSendTime).TotalMilliseconds > config.ConnectionRequestMinResendDelay && connections[i].HandshakeResendAttempts < config.MaxConnectionRequestResends)
                        {
                            connections[i].HandshakeResendAttempts++;
                            connections[i].HandshakeLastSendTime = NetTime.Now;

                            // Calculate the minimum size we can fit the packet in
                            int minSize = 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (config.TimeBasedConnectionChallenge ? sizeof(ulong) * 3 : 0);

                            // Calculate the actual size with respect to amplification padding
                            int size = Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding);

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ConnectionRequest, false);

                            // Copy the identification token
                            Buffer.BlockCopy(Constants.RUFFLES_PROTOCOL_IDENTIFICATION, 0, memory.Buffer, 1, Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length);

                            if (config.TimeBasedConnectionChallenge)
                            {
                                // Write the response unix time
                                for (byte x = 0; x < sizeof(ulong); x++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + x] = ((byte)(connections[i].PreConnectionChallengeTimestamp >> (x * 8)));

                                // Write counter
                                for (byte x = 0; x < sizeof(ulong); x++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + x] = ((byte)(connections[i].PreConnectionChallengeCounter >> (x * 8)));

                                // Write IV
                                for (byte x = 0; x < sizeof(ulong); x++) memory.Buffer[1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + x] = ((byte)(connections[i].PreConnectionChallengeIV >> (x * 8)));

                                // Print debug
                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Resending ConnectionRequest with challenge [Counter=" + connections[i].PreConnectionChallengeCounter + "] [IV=" + connections[i].PreConnectionChallengeIV + "] [Time=" + connections[i].PreConnectionChallengeTimestamp + "] [Hash=" + HashProvider.GetStableHash64(connections[i].PreConnectionChallengeTimestamp, connections[i].PreConnectionChallengeCounter, connections[i].PreConnectionChallengeIV) + "]");
                            }
                            else
                            {
                                // Print debug
                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Resending ConnectionRequest");
                            }

                            connections[i].SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);
                        }
                    }
                    else if (connections[i].State == ConnectionState.RequestingChallenge)
                    {
                        if ((NetTime.Now - connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeResendDelay && connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            connections[i].HandshakeResendAttempts++;
                            connections[i].HandshakeLastSendTime = NetTime.Now;

                            // Packet size
                            uint size = 1 + sizeof(ulong) + 1;

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory(size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeRequest, false);

                            // Write connection challenge
                            for (byte x = 0; x < sizeof(ulong); x++) memory.Buffer[1 + x] = ((byte)(connections[i].ConnectionChallenge >> (x * 8)));

                            // Write the connection difficulty
                            memory.Buffer[1 + sizeof(ulong)] = connections[i].ChallengeDifficulty;

                            // Print debug
                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Resending ChallengeRequest");

                            // Send the challenge
                            connections[i].SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);
                        }
                    }
                    else if (connections[i].State == ConnectionState.SolvingChallenge)
                    {
                        if ((NetTime.Now - connections[i].HandshakeLastSendTime).TotalMilliseconds > config.HandshakeResendDelay && connections[i].HandshakeResendAttempts < config.MaxHandshakeResends)
                        {
                            connections[i].HandshakeResendAttempts++;
                            connections[i].HandshakeLastSendTime = NetTime.Now;

                            // Calculate the minimum size we can fit the packet in
                            int minSize = 1 + sizeof(ulong);

                            // Calculate the actual size with respect to amplification padding
                            int size = Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding);

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeResponse, false);

                            // Write the challenge response
                            for (byte x = 0; x < sizeof(ulong); x++) memory.Buffer[1 + x] = ((byte)(connections[i].ChallengeAnswer >> (x * 8)));

                            // Print debug
                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Resending ChallengeResponse");

                            // Send the challenge response
                            connections[i].SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);
                        }
                    }
                    else if (connections[i].State == ConnectionState.Connected)
                    {
                        if (!connections[i].HailStatus.Completed && (NetTime.Now - connections[i].HailStatus.LastAttempt).TotalMilliseconds > config.HandshakeResendDelay && connections[i].HailStatus.Attempts < config.MaxHandshakeResends)
                        {
                            connections[i].HailStatus.Attempts++;
                            connections[i].HailStatus.LastAttempt = NetTime.Now;

                            // Packet size
                            int size = 2 + (byte)config.ChannelTypes.Length;

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Hail, false);

                            // Write the amount of channels
                            memory.Buffer[1] = (byte)config.ChannelTypes.Length;

                            // Write the channel types
                            for (byte x = 0; x < (byte)config.ChannelTypes.Length; x++)
                            {
                                memory.Buffer[2 + x] = (byte)config.ChannelTypes[x];
                            }

                            // Print debug
                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Resending Hail");

                            connections[i].SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);
                        }
                    }
                }
            }
        }

        private void CheckConnectionTimeouts()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead)
                {
                    if (connections[i].State == ConnectionState.RequestingConnection)
                    {
                        if ((NetTime.Now - connections[i].ConnectionStarted).TotalMilliseconds > config.ConnectionRequestTimeout)
                        {
                            // This client has taken too long to connect. Let it go.
                            DisconnectInternal(connections[i], false, true);
                        }
                    }
                    else if (connections[i].State != ConnectionState.Connected)
                    {
                        // They are not requesting connection. But they are not connected. This means they are doing a handshake
                        if ((NetTime.Now - connections[i].HandshakeStarted).TotalMilliseconds > config.HandshakeTimeout)
                        {
                            // This client has taken too long to connect. Let it go.
                            DisconnectInternal(connections[i], false, true);
                        }
                    }
                    else
                    {
                        if ((NetTime.Now - connections[i].LastMessageIn).TotalMilliseconds > config.ConnectionTimeout)
                        {
                            // This client has not answered us in way too long. Let it go
                            DisconnectInternal(connections[i], false, true);
                        }
                        else if ((NetTime.Now - connections[i].ConnectionStarted).TotalMilliseconds > config.ConnectionQualityGracePeriod)
                        {
                            // They are no longer covered by connection quality grace. Check their ping and packet loss

                            if ((connections[i].OutgoingConfirmedPackets > 128 || connections[i].OutgoingResentPackets > 128) && (1 - (double)connections[i].OutgoingConfirmedPackets / connections[i].OutgoingResentPackets) > config.MaxPacketLossPercentage)
                            {
                                // They have too high of a packet drop. Disconnect them
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Disconnecting client because their packetLoss is too large. [OCP=" + connections[i].OutgoingConfirmedPackets + "] [ORP=" + connections[i].OutgoingResentPackets + "]");
                                DisconnectInternal(connections[i], false, true);
                            }
                            else if (connections[i].SmoothRoundtrip > config.MaxRoundtripTime)
                            {
                                // They have too high of a roundtrip time. Disconnect them
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Disconnecting client because their roundTripTime is too large. [SRTT=" + connections[i].SmoothRoundtrip + "] [RTT=" + connections[i].Roundtrip + "]");
                                DisconnectInternal(connections[i], false, true);
                            }
                        }
                    }
                }
            }
        }

        private void CheckConnectionHeartbeats()
        {
            for (int i = 0; i < connections.Length; i++)
            {
                if (connections[i] != null && !connections[i].Dead && connections[i].State == ConnectionState.Connected)
                {
                    if ((NetTime.Now - connections[i].LastMessageOut).TotalMilliseconds > config.HeartbeatDelay)
                    {
                        // This client has not been talked to in a long time. Send a heartbeat.

                        // Create sequenced heartbeat packet
                        HeapMemory heartbeatMemory = connections[i].HeartbeatChannel.CreateOutgoingHeartbeatMessage();

                        // Send heartbeat
                        connections[i].SendRaw(new ArraySegment<byte>(heartbeatMemory.Buffer, (int)heartbeatMemory.VirtualOffset, (int)heartbeatMemory.VirtualCount), false, (ushort)heartbeatMemory.VirtualCount);

                        // DeAlloc the memory
                        memoryManager.DeAlloc(heartbeatMemory);
                    }
                }
            }
        }

        private void PollInternalIOQueue()
        {
            while (internalEventQueue.TryDequeue(out InternalEvent @event))
            {
                if (@event.Type == InternalEvent.InternalEventType.Connect)
                {
                    // Send connection
                    ConnectInternal(@event.Endpoint, @event.PreConnectionChallengeTimestamp, @event.PreConnectionChallengeCounter, @event.PreConnectionChallengeIV);
                }
                else if (@event.Type == InternalEvent.InternalEventType.Disconnect)
                {
                    // Disconnect
                    DisconnectInternal(@event.Connection, @event.SendMessage, false);
                }
                else if (@event.Type == InternalEvent.InternalEventType.Send)
                {
                    // Send the data
                    PacketHandler.SendMessage(new ArraySegment<byte>(@event.Data.Buffer, (int)@event.Data.VirtualOffset, (int)@event.Data.VirtualCount), @event.Connection, @event.ChannelId, @event.NoMerge, memoryManager);

                    // Dealloc the memory
                    memoryManager.DeAlloc(@event.Data);
                }
            }
        }

        private EndPoint _fromIPv4Endpoint = new IPEndPoint(IPAddress.Any, 0);
        private EndPoint _fromIPv6Endpoint = new IPEndPoint(IPAddress.IPv6Any, 0);

        private readonly List<Socket> _selectSockets = new List<Socket>();
        private readonly Stopwatch _selectWatch = new Stopwatch();
        private void InternalPollSocket(int ms)
        {
            _selectWatch.Reset();
            _selectWatch.Start();

            do
            {
                _selectSockets.Clear();

                if (ipv4Socket != null)
                {
                    _selectSockets.Add(ipv4Socket);
                }

                if (ipv6Socket != null)
                {
                    _selectSockets.Add(ipv6Socket);
                }

                int sleepTime = (ms - (int)_selectWatch.ElapsedMilliseconds) * 1000;

                // Check what sockets have data
                if (sleepTime > 0)
                {
                    Socket.Select(_selectSockets, null, null, sleepTime);

                    // Iterate the sockets with data
                    for (int i = 0; i < _selectSockets.Count; i++)
                    {
                        try
                        {
                            do
                            {
                                // Get a endpoint reference
                                EndPoint _endpoint = _selectSockets[i].AddressFamily == AddressFamily.InterNetwork ? _fromIPv4Endpoint : _selectSockets[i].AddressFamily == AddressFamily.InterNetworkV6 ? _fromIPv6Endpoint : null;

                                int size = _selectSockets[i].ReceiveFrom(incomingBuffer, 0, incomingBuffer.Length, SocketFlags.None, ref _endpoint);

                                HandlePacket(new ArraySegment<byte>(incomingBuffer, 0, size), _endpoint, true);
                            } while (_selectSockets[i].Available > 0);
                        }
                        catch (Exception e)
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when receiving from socket: " + e);
                        }
                    }
                }
            } while (_selectWatch.ElapsedMilliseconds < ms);

            _selectWatch.Stop();
        }

        /// <summary>
        /// Polls the RuffleSocket for incoming events about connections.
        /// </summary>
        /// <returns>The poll result.</returns>
        public NetworkEvent Poll()
        {
            if (userEventQueue.TryDequeue(out NetworkEvent @event))
            {
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
                SocketReceiveTime = NetTime.Now,
                MemoryManager = memoryManager
            };
        }

        internal void SendRaw(Connection connection, ArraySegment<byte> payload, bool noMerge, ushort headerBytes)
        {
            connection.LastMessageOut = NetTime.Now;

            bool merged = false;

            if (!config.EnablePacketMerging || noMerge || !(merged = connection.Merger.TryWrite(payload, headerBytes)))
            {
                connection.OutgoingTotalBytes += (ulong)payload.Count;
                connection.OutgoingUserBytes += (ulong)payload.Count - headerBytes;

                connection.OutgoingPackets++;

                if (!merged)
                {
                    connection.OutgoingWirePackets++;
                }

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
            try
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
            catch (Exception e)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when sending through socket: " + e);
            }
        }

        private void SendRawRealEndPoint(IPEndPoint endpoint, ArraySegment<byte> payload)
        {
            try
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
            catch (Exception e)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when sending through socket: " + e);
            }
        }


        private readonly List<ArraySegment<byte>> _mergeSegmentResults = new List<ArraySegment<byte>>();
        internal void HandlePacket(ArraySegment<byte> payload, EndPoint endpoint, bool allowMergeUnpack, bool wirePacket = true)
        {
            if (payload.Count < 1)
            {
                // Invalid size
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Got packet of size " + payload.Count + " from " + endpoint + ". Packet is too small");
                return;
            }

            // Unpack header, dont cast to MessageType enum for safety
            HeaderPacker.Unpack(payload.Array[payload.Offset], out byte messageType, out bool fragmented);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Unpacked packet. [MessageType=" + (MessageType)messageType + "] [Fragmented=" + fragmented + "]");

            switch (messageType)
            {
                case (byte)MessageType.Merge:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingWirePackets++;

                            connection.IncomingTotalBytes += (ulong)payload.Count;

                            if (!config.EnablePacketMerging)
                            {
                                // Big missmatch here.
                                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Packet was merged but packet merging was disabled. Skipping merge packet");
                                return;
                            }

                            if (!allowMergeUnpack)
                            {
                                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Packet was double merged. Skipping nested merge packet");
                                return;
                            }

                            // Unpack the merged packet
                            MessageMerger.Unpack(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), _mergeSegmentResults);

                            if (_mergeSegmentResults != null)
                            {
                                for (int i = 0; i < _mergeSegmentResults.Count; i++)
                                {
                                    // Handle the segment
                                    HandlePacket(_mergeSegmentResults[i], endpoint, false, false);
                                }
                            }
                        }
                    }
                    break;
                case (byte)MessageType.ConnectionRequest:
                    {
                        if (payload.Count < config.AmplificationPreventionHandshakePadding || payload.Count < 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length)
                        {
                            // This message is too small. They might be trying to use us for amplification.
                            return;
                        }

                        for (int i = 0; i < Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length; i++)
                        {
                            if (payload.Array[payload.Offset + 1 + i] != Constants.RUFFLES_PROTOCOL_IDENTIFICATION[i])
                            {
                                // The identification number did not match
                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Connection request packet was filtered away. Identification did not match");
                                return;
                            }
                        }

                        if (config.TimeBasedConnectionChallenge)
                        {
                            // Get the current unix time seconds
                            ulong currentUnixTime = (ulong)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;

                            // Read the time they used
                            ulong challengeUnixTime = (((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length]) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 1] << 8) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 2] << 16) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 3] << 24) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 4] << 32) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 5] << 40) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 6] << 48) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + 7] << 56));

                            // The seconds diff
                            long secondsDiff = (long)currentUnixTime - (long)challengeUnixTime;

                            if (secondsDiff > (long)config.ConnectionChallengeTimeWindow || secondsDiff < -(long)config.ConnectionChallengeTimeWindow)
                            {
                                // Outside the allowed window
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogWarning("Client " + endpoint + " failed the connection request. They were outside of their allowed window. The diff was " + Math.Abs(secondsDiff) + " seconds");
                                return;
                            }

                            // Read the counter they used to collide the hash
                            ulong counter = (((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong)]) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 1] << 8) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 2] << 16) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 3] << 24) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 4] << 32) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 5] << 40) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 6] << 48) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + sizeof(ulong) + 7] << 56));

                            // Read the initialization vector they used
                            ulong userIv = (((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2)]) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 1] << 8) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 2] << 16) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 3] << 24) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 4] << 32) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 5] << 40) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 6] << 48) |
                                            ((ulong)payload.Array[payload.Offset + 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (sizeof(ulong) * 2) + 7] << 56));

                            // Ensure they dont reuse a IV
                            if (challengeInitializationVectors[userIv])
                            {
                                // This IV is being reused.
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogWarning("Client " + endpoint + " failed the connection request. They were trying to reuse an IV");
                                return;
                            }

                            // Calculate the hash the user claims have a collision
                            ulong claimedHash = HashProvider.GetStableHash64(challengeUnixTime, counter, userIv);

                            // Check if the hash collides
                            bool isCollided = ((claimedHash << (sizeof(ulong) * 8 - config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - config.ChallengeDifficulty)) == 0;

                            if (!isCollided)
                            {
                                // They failed the challenge
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogWarning("Client " + endpoint + " failed the connection request. They submitted an invalid answer. [ClaimedHash=" + claimedHash + "] [Counter=" + counter + "] [IV=" + userIv + "] [Time=" + challengeUnixTime + "]");
                                return;
                            }

                            // Save the IV to the sliding window
                            challengeInitializationVectors[userIv] = true;
                        }

                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " is being challenged");

                        Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingChallenge);

                        if (connection != null)
                        {
                            // This connection was successfully added as pending

                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            // Set resend values
                            connection.HandshakeResendAttempts = 1;
                            connection.HandshakeLastSendTime = NetTime.Now;

                            // Packet size
                            uint size = 1 + sizeof(ulong) + 1;

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory(size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeRequest, false);

                            // Write connection challenge
                            for (byte i = 0; i < sizeof(ulong); i++) memory.Buffer[1 + i] = ((byte)(connection.ConnectionChallenge >> (i * 8)));

                            // Write the challenge difficulty
                            memory.Buffer[1 + sizeof(ulong)] = connection.ChallengeDifficulty;

                            // Send the challenge
                            connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);

                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " was sent a challenge of difficulty " + connection.ChallengeDifficulty);
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Client " + endpoint + " could not be challenged. Allocation failed");
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeRequest:
                    {
                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingConnection)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            if (payload.Count < 10)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Server " + endpoint + " sent us a payload that was too small. Disconnecting");
                                DisconnectInternal(connection, false, false);
                                return;
                            }

                            connection.HandshakeStarted = NetTime.Now;
                            connection.LastMessageIn = NetTime.Now;

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
                            connection.HandshakeLastSendTime = NetTime.Now;
                            connection.State = ConnectionState.SolvingChallenge;

                            // Calculate the minimum size we can fit the packet in
                            int minSize = 1 + sizeof(ulong);

                            // Calculate the actual size with respect to amplification padding
                            int size = Math.Max(minSize, (int)config.AmplificationPreventionHandshakePadding);

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.ChallengeResponse, false);

                            // Write the challenge response
                            for (byte i = 0; i < sizeof(ulong); i++) memory.Buffer[1 + i] = ((byte)(additionsRequired >> (i * 8)));

                            // Send the challenge response
                            connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);

                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Server " + endpoint + " challenge of difficulty " + connection.ChallengeDifficulty + " was solved. Answer was sent. [CollidedValue=" + collidedValue + "]");
                        }
                    }
                    break;
                case (byte)MessageType.ChallengeResponse:
                    {
                        if (payload.Count < config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " sent a challenge response that was smaller than the amplification padding");
                            return;
                        }

                        Connection connection = GetPendingConnection(endpoint);

                        if (connection != null && connection.State == ConnectionState.RequestingChallenge)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            if (payload.Count < 9)
                            {
                                // The message is not large enough to contain all the data neccecary. Wierd server?
                                DisconnectInternal(connection, false, false);
                                return;
                            }

                            connection.LastMessageIn = NetTime.Now;

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

                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " successfully completed challenge of difficulty " + connection.ChallengeDifficulty);

                                ConnectPendingConnection(connection);

                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " state changed to connected");

                                connection.HailStatus.Attempts = 1;
                                connection.HailStatus.HasAcked = false;
                                connection.HailStatus.LastAttempt = NetTime.Now;

                                // Packet size
                                int size = 2 + (byte)config.ChannelTypes.Length;

                                // Allocate memory
                                HeapMemory memory = memoryManager.AllocHeapMemory((uint)size);

                                // Write the header
                                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Hail, false);

                                // Write the amount of channels
                                memory.Buffer[1] = (byte)config.ChannelTypes.Length;

                                // Write the channel types
                                for (byte i = 0; i < (byte)config.ChannelTypes.Length; i++)
                                {
                                    memory.Buffer[2 + i] = (byte)config.ChannelTypes[i];
                                }

                                // Send the response
                                connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                                // Release memory
                                memoryManager.DeAlloc(memory);

                                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " was sent a hail");

                                // Send to userspace
                                PublishEvent(new NetworkEvent()
                                {
                                    Connection = connection,
                                    Socket = this,
                                    Type = NetworkEventType.Connect,
                                    AllowUserRecycle = false,
                                    ChannelId = 0,
                                    Data = new ArraySegment<byte>(),
                                    InternalMemory = null,
                                    SocketReceiveTime = NetTime.Now,
                                    MemoryManager = memoryManager
                                });
                            }
                            else
                            {
                                // Failed, disconnect them
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " failed the challenge. Disconnecting");

                                DisconnectInternal(connection, false, false);
                            }
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " sent a challenge response but they were either not connected or were not in a RequestingChallenge state. Delayed packets?");
                        }
                    }
                    break;
                case (byte)MessageType.Hail:
                    {
                        Connection pendingConnection = GetPendingConnection(endpoint);
                        Connection connectedConnection = GetConnection(endpoint);

                        if (pendingConnection != null)
                        {
                            pendingConnection.IncomingPackets++;

                            if (wirePacket)
                            {
                                pendingConnection.IncomingWirePackets++;
                                pendingConnection.IncomingTotalBytes += (ulong)payload.Count;
                           }
                        }

                        if (connectedConnection != null)
                        {
                            connectedConnection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connectedConnection.IncomingWirePackets++;
                                connectedConnection.IncomingTotalBytes += (ulong)payload.Count;
                            }
                        }

                        if (connectedConnection != null && connectedConnection.State == ConnectionState.Connected)
                        {
                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory(1);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.HailConfirmed, false);

                            // Send confirmation
                            connectedConnection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);

                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Hail confirmation sent to " + endpoint);
                        }
                        else if (pendingConnection != null && pendingConnection.State == ConnectionState.SolvingChallenge)
                        {
                            if (payload.Count < 2)
                            {
                                // Invalid size.
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent a payload that was too small. Disconnecting");

                                DisconnectInternal(pendingConnection, false, false);
                                return;
                            }

                            pendingConnection.LastMessageIn = NetTime.Now;

                            // Read the amount of channels
                            byte channelCount = payload.Array[payload.Offset + 1];

                            if (payload.Count < channelCount + 2)
                            {
                                // Invalid size.
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent a payload that was too small. Disconnecting");
                                DisconnectInternal(pendingConnection, false, false);
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
                                    case (byte)ChannelType.UnreliableOrdered:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.UnreliableOrdered;
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
                                    case (byte)ChannelType.ReliableOrdered:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.ReliableOrdered;
                                        }
                                        break;
                                    case (byte)ChannelType.ReliableFragmented:
                                        {
                                            pendingConnection.ChannelTypes[i] = ChannelType.ReliableFragmented;
                                        }
                                        break;
                                    default:
                                        {
                                            // Unknown channel type. Disconnect.
                                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                            DisconnectInternal(pendingConnection, false, false);
                                            return;
                                        }
                                }
                            }

                            // Alloc the channels array
                            pendingConnection.Channels = new IChannel[channelCount];

                            // Alloc the channels
                            for (byte i = 0; i < pendingConnection.ChannelTypes.Length; i++)
                            {
                                IChannel channel = channelPool.GetChannel(pendingConnection.ChannelTypes[i], i, pendingConnection, config, memoryManager);

                                if (channel == null)
                                {
                                    // Unknown channel type. Disconnect.
                                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                    DisconnectInternal(pendingConnection, false, false);
                                    return;
                                }

                                pendingConnection.Channels[i] = channel;
                            }

                            // Set state to connected
                            ConnectPendingConnection(pendingConnection);

                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " state changed to connected");

                            // Send to userspace
                            PublishEvent(new NetworkEvent()
                            {
                                Connection = pendingConnection,
                                Socket = this,
                                Type = NetworkEventType.Connect,
                                AllowUserRecycle = false,
                                ChannelId = 0,
                                Data = new ArraySegment<byte>(),
                                InternalMemory = null,
                                SocketReceiveTime = NetTime.Now,
                                MemoryManager = memoryManager
                            });

                            // Allocate memory
                            HeapMemory memory = memoryManager.AllocHeapMemory(1);

                            // Write the header
                            memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.HailConfirmed, false);

                            // Send confirmation
                            pendingConnection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                            // Release memory
                            memoryManager.DeAlloc(memory);

                            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " was sent hail confimrations");
                        }
                    }
                    break;
                case (byte)MessageType.HailConfirmed:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            if (!connection.HailStatus.Completed && connection.State == ConnectionState.Connected)
                            {
                                connection.HailStatus.HasAcked = true;
                            }
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Heartbeat:
                    {
                        if (!config.EnableHeartbeats)
                        {
                            // TODO: Handle
                            // This is a missmatch.
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Heartbeat received from " + endpoint + " but the we do not have heartbeats enabled. Configuration missmatch?");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            // Heartbeats are sequenced to not properly handle network congestion

                            HeapPointers pointers = connection.HeartbeatChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), out byte headerBytes);

                            if (pointers != null)
                            {
                                MemoryWrapper wrapper = (MemoryWrapper)pointers.Pointers[0];

                                if (wrapper != null)
                                {
                                    if (wrapper.AllocatedMemory != null)
                                    {
                                        connection.LastMessageIn = NetTime.Now;

                                        // Dealloc the memory
                                        memoryManager.DeAlloc(wrapper.AllocatedMemory);
                                    }

                                    if (wrapper.DirectMemory != null)
                                    {
                                        connection.LastMessageIn = NetTime.Now;
                                    }

                                    // Dealloc the wrapper
                                    memoryManager.DeAlloc(wrapper);
                                }

                                // Dealloc the pointers
                                memoryManager.DeAlloc(pointers);
                            }
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Data:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            connection.LastMessageIn = NetTime.Now;

                            PacketHandler.HandleIncomingMessage(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1), connection, config, memoryManager);
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Ack:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            connection.LastMessageIn = NetTime.Now;

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
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.Disconnect:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.IncomingPackets++;

                            if (wirePacket)
                            {
                                connection.IncomingWirePackets++;
                                connection.IncomingTotalBytes += (ulong)payload.Count;
                            }

                            connection.Disconnect(false);
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
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
                            PublishEvent(new NetworkEvent()
                            {
                                Connection = null,
                                Socket = this,
                                Type = NetworkEventType.UnconnectedData,
                                AllowUserRecycle = true,
                                Data = new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount),
                                InternalMemory = memory,
                                SocketReceiveTime = NetTime.Now,
                                ChannelId = 0,
                                MemoryManager = memoryManager
                            });
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Got unconnected message but SocketConfig.AllowUnconnectedMessages is disabled.");
                        }
                    }
                    break;
                case (byte)MessageType.MTURequest:
                    {
                        if (config.EnablePathMTU)
                        {
                            Connection connection = GetConnection(endpoint);

                            if (connection != null)
                            {
                                // Alloc memory for response
                                HeapMemory memory = memoryManager.AllocHeapMemory((uint)payload.Count);

                                // Write the header
                                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.MTUResponse, false);

                                // Send the response
                                connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                                // Dealloc the memory
                                memoryManager.DeAlloc(memory);
                            }
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                        }
                    }
                    break;
                case (byte)MessageType.MTUResponse:
                    {
                        if (config.EnablePathMTU)
                        {
                            Connection connection = GetConnection(endpoint);

                            if (connection != null)
                            {
                                connection.IncomingPackets++;

                                if (wirePacket)
                                {
                                    connection.IncomingWirePackets++;
                                    connection.IncomingTotalBytes += (ulong)payload.Count;
                                }

                                // Calculate the new MTU
                                uint attemptedMtu = (uint)(connection.MTU * config.MTUGrowthFactor);

                                if (attemptedMtu > config.MaximumMTU)
                                {
                                    attemptedMtu = config.MaximumMTU;
                                }

                                if (attemptedMtu < config.MinimumMTU)
                                {
                                    attemptedMtu = config.MinimumMTU;
                                }

                                if (attemptedMtu == payload.Count)
                                {
                                    // This is a valid response

                                    // Set new MTU
                                    connection.MTU = (ushort)attemptedMtu;

                                    // Set new status
                                    connection.MTUStatus = new MessageStatus()
                                    {
                                        Attempts = 0,
                                        HasAcked = false,
                                        LastAttempt = NetTime.MinValue
                                    };

                                    if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " MTU was increased to " + connection.MTU);
                                }
                            }
                            else
                            {
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " connection could not be found");
                            }
                        }
                    }
                    break;
            }
        }

        internal void ConnectPendingConnection(Connection connection)
        {
            // Lock to prevent modifying a half dead connection
            lock (connectionAddRemoveLock)
            {
                // Remove it from pending
                addressPendingConnectionLookup.Remove(connection.EndPoint);
                addressConnectionLookup.Add(connection.EndPoint, connection);

                connection.ConnectionCompleted = NetTime.Now;
                connection.State = ConnectionState.Connected;

                pendingConnections++;
            }
        }

        internal Connection GetPendingConnection(EndPoint endpoint)
        {
            // Lock to prevent grabbing a half dead connection
            lock (connectionAddRemoveLock)
            {
                if (addressPendingConnectionLookup.ContainsKey(endpoint))
                {
                    return addressPendingConnectionLookup[endpoint];
                }
                else
                {
                    return null;
                }
            }
        }

        internal Connection GetConnection(EndPoint endpoint)
        {
            // Lock to prevent grabbing a half dead connection
            lock (connectionAddRemoveLock)
            {
                if (addressConnectionLookup.ContainsKey(endpoint))
                {
                    return addressConnectionLookup[endpoint];
                }
                else
                {
                    return null;
                }
            }
        }

        internal void DisconnectInternal(Connection connection, bool sendMessage, bool timeout)
        {
            if (connection.State == ConnectionState.Connected && sendMessage && !timeout)
            {
                // Send disconnect message

                // Allocate memory
                HeapMemory memory = memoryManager.AllocHeapMemory(1);

                // Write disconnect header
                memory.Buffer[0] = HeaderPacker.Pack((byte)MessageType.Disconnect, false);

                // Send disconnect message
                connection.SendRaw(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true, (ushort)memory.VirtualCount);

                // Release memory
                memoryManager.DeAlloc(memory);
            }

            // Lock when removing the connection to prevent another thread grabbing it before its fully dead.
            lock (connectionAddRemoveLock)
            {
                if (config.ReuseConnections)
                {
                    // Mark as dead, this will allow it to be reclaimed. (Note: do this first to prevent it being grabbed before its cleaned up)
                    connection.Dead = true;
                }
                else
                {
                    // Release to GC unless user has a hold of it
                    connections[connection.Id] = null;
                }

                // Remove connection lookups
                if (connection.State != ConnectionState.Connected)
                {
                    addressPendingConnectionLookup.Remove(connection.EndPoint);

                    pendingConnections--;
                }
                else
                {
                    addressConnectionLookup.Remove(connection.EndPoint);
                }

                // Set the state to disconnected
                connection.State = ConnectionState.Disconnected;

                if (config.EnableHeartbeats)
                {
                    // Release all memory from the heartbeat channel
                    connection.HeartbeatChannel.Release();
                }

                if (config.EnablePacketMerging)
                {
                    // Clean the merger
                    connection.Merger.Clear();
                }

                // Reset all channels, releasing memory etc
                for (int i = 0; i < connection.Channels.Length; i++)
                {
                    if (connection.Channels[i] != null)
                    {
                        // Grab a ref to the channel
                        IChannel channel = connection.Channels[i];
                        // Set the channel to null. Preventing further polls
                        connection.Channels[i] = null;
                        // Return old channel to pool
                        channelPool.Return(channel);
                    }
                }

                // Send disconnect to userspace
                PublishEvent(new NetworkEvent()
                {
                    Connection = connection,
                    Socket = this,
                    Type = timeout ? NetworkEventType.Timeout : NetworkEventType.Disconnect,
                    AllowUserRecycle = false,
                    ChannelId = 0,
                    Data = new ArraySegment<byte>(),
                    InternalMemory = null,
                    SocketReceiveTime = NetTime.Now,
                    MemoryManager = memoryManager
                });
            }
        }

        internal Connection AddNewConnection(EndPoint endpoint, ConnectionState state)
        {
            // Lock when adding connection to prevent grabbing a half alive connection.
            lock (connectionAddRemoveLock)
            {
                // Make sure they are not already connected to prevent an attack where a single person can fill all the slots.
                if (addressPendingConnectionLookup.ContainsKey(endpoint) || addressConnectionLookup.ContainsKey(endpoint) || pendingConnections > config.MaxPendingConnections)
                {
                    return null;
                }

                Connection connection = null;

                for (ushort i = 0; i < connections.Length; i++)
                {
                    if (connections[i] == null)
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
                            LastMessageIn = NetTime.Now,
                            LastMessageOut = NetTime.Now,
                            ConnectionStarted = NetTime.Now,
                            HandshakeStarted = NetTime.Now,
                            ConnectionCompleted = NetTime.Now,
                            HandshakeResendAttempts = 0,
                            ChallengeAnswer = 0,
                            Channels = new IChannel[0],
                            ChannelTypes = new ChannelType[0],
                            HandshakeLastSendTime = NetTime.Now,
                            Merger = config.EnablePacketMerging ? new MessageMerger(config.MaxMergeMessageSize, config.MaxMergeDelay) : null,
                            MTU = config.MinimumMTU,
                            SmoothRoundtrip = 0,
                            RoundtripVarience = 0,
                            HighestRoundtripVarience = 0,
                            Roundtrip = 500,
                            LowestRoundtrip = 500
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
                            IChannel channel = channelPool.GetChannel(config.ChannelTypes[x], x, connection, config, memoryManager);

                            if (channel == null)
                            {
                                // Unknown channel type. Disconnect.
                                // TODO: Fix
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " sent an invalid ChannelType. Disconnecting");
                                DisconnectInternal(connection, false, false);
                            }

                            connection.Channels[x] = channel;
                        }

                        connections[i] = connection;
                        addressPendingConnectionLookup.Add(endpoint, connection);

                        pendingConnections++;

                        break;
                    }
                    else if (connections[i].Dead && connections[i].Recycled)
                    {
                        // This is no longer used, reuse it
                        connection = connections[i];

                        // Reset the core connection, including statistics and other data
                        connection.Reset();

                        // Set the Id
                        connection.Id = i;

                        // Set the state
                        connection.State = state;

                        // Set the socket
                        connection.Socket = this;

                        // Set the endpoint
                        connection.EndPoint = endpoint;

                        // Generate a new challenge
                        connection.ConnectionChallenge = RandomProvider.GetRandomULong();

                        // Set the difficulty
                        connection.ChallengeDifficulty = config.ChallengeDifficulty;

                        // Reset the MTU
                        connection.MTU = config.MinimumMTU;

                        // Set all the times to now (to prevent instant timeout)
                        connection.LastMessageOut = NetTime.Now;
                        connection.LastMessageIn = NetTime.Now;
                        connection.ConnectionStarted = NetTime.Now;
                        connection.HandshakeStarted = NetTime.Now;
                        connection.HandshakeLastSendTime = NetTime.Now;
                        connection.ConnectionCompleted = NetTime.Now;

                        addressPendingConnectionLookup.Add(endpoint, connection);

                        pendingConnections++;

                        break;
                    }
                }

                return connection;
            }
        }
    }
}
