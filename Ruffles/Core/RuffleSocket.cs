using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Ruffles.Channeling;
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

            if (Config.EnablePollEvents)
            {
                delivered = true;
                userEventQueue.Enqueue(@event);
            }

            if (Config.EnableCallbackEvents && OnNetworkEvent != null)
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
        private readonly Dictionary<EndPoint, Connection> addressConnectionLookup = new Dictionary<EndPoint, Connection>();
        private Connection HeadConnection;

        // Lock for adding or removing connections. This is done to allow for a quick ref to be gained on the user thread when connecting.
        private readonly ReaderWriterLockSlim connectionsLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        // TODO: Removed hardcoded size
        private readonly ConcurrentCircularQueue<NetworkEvent> userEventQueue;
        private readonly ConcurrentCircularQueue<InternalEvent> internalEventQueue;

        private Socket ipv4Socket;
        private Socket ipv6Socket;
        private static readonly bool SupportsIPv6 = Socket.OSSupportsIPv6;

        private readonly SlidingSet<ulong> challengeInitializationVectors;

        internal readonly MemoryManager MemoryManager;
        internal readonly NetworkSimulator Simulator;
        internal readonly ChannelPool ChannelPool;
        internal readonly SocketConfig Config;

        private Thread _logicThread;
        private Thread _socketThread;
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
            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Checking SocketConfig validity...");

            List<string> configurationErrors = config.GetInvalidConfiguration();

            if (configurationErrors.Count > 0)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Invalid configuration! Please fix the following issues [" + string.Join(",", configurationErrors.ToArray()) + "]");
            }
            else
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("SocketConfig is valid");
            }

            this.Config = config;

            if (config.UseSimulator)
            {
                Simulator = new NetworkSimulator(config.SimulatorConfig, SendRaw);
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

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating " + config.ConnectionChallengeHistory + " challenge IV slots");
            challengeInitializationVectors = new SlidingSet<ulong>((int)config.ConnectionChallengeHistory, true);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating memory manager");
            MemoryManager = new MemoryManager(config);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Allocating channel pool");
            ChannelPool = new ChannelPool(config);

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
                bool bindSuccess = Bind(Config.IPv4ListenAddress, Config.IPv6ListenAddress, Config.DualListenPort, Config.UseIPv6Dual);

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
            _logicThread = new Thread(StartNetworkLogic)
            {
                Name = "NetworkThread",
                IsBackground = true
            };

            // Create network thread
            _socketThread = new Thread(StartReceiveLogic)
            {
                Name = "SocketThread",
                IsBackground = true
            };

            // Set running state to true
            IsRunning = true;

            // Start network thread
            _logicThread.Start();
            _socketThread.Start();

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
            for (Connection connection = HeadConnection; connection != null; connection = connection.NextConnection)
            {
                connection.Disconnect(true, false);
            }

            IsRunning = false;
            _logicThread.Join();
            _socketThread.Join();
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
            if (connection != null && connection.State == ConnectionState.Connected)
            {
                connection.HandleDelayedChannelSend(payload, channelId, noMerge);

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
        /// <param name="connection">The connection to send to.</param>
        /// <param name="channelId">The channel index to send the payload over.</param>
        /// <param name="noMerge">If set to <c>true</c> the message will not be merged.</param>
        public bool SendLater(ArraySegment<byte> payload, Connection connection, byte channelId, bool noMerge)
        {
            if (!Config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call SendLater when EnableQueuedIOEvents is disabled");
            }

            if (connection != null && connection.State == ConnectionState.Connected)
            {
                // Alloc memory
                HeapMemory memory = MemoryManager.AllocHeapMemory((uint)payload.Count);

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
        /// Sends an unconnected message.
        /// </summary>
        /// <param name="payload">Payload.</param>
        /// <param name="endpoint">Endpoint.</param>
        public void SendUnconnected(ArraySegment<byte> payload, IPEndPoint endpoint)
        {
            if (payload.Count > Config.MinimumMTU)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Error)  Logging.LogError("Tried to send unconnected message that was too large. [Size=" + payload.Count + "] [MaxMessageSize=" + Config.MaxFragments + "]");
                return;
            }

            // Allocate the memory
            HeapMemory memory = MemoryManager.AllocHeapMemory((uint)payload.Count + 1);

            // Write headers
            memory.Buffer[0] = HeaderPacker.Pack(MessageType.UnconnectedData);

            // Copy payload to borrowed memory
            Buffer.BlockCopy(payload.Array, payload.Offset, memory.Buffer, 1, payload.Count);

            // Send the packet
            SendRaw(endpoint, new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount));

            // Release memory
            MemoryManager.DeAlloc(memory);
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

            if (Config.TimeBasedConnectionChallenge)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Calculating with difficulty " + Config.ChallengeDifficulty);

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
                while ((hash << (sizeof(ulong) * 8 - Config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - Config.ChallengeDifficulty) != 0);

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
            if (!Config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call ConnectLater when EnableQueuedIOEvents is disabled");
            }

            if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogInfo("Attempting to connect (later) to " + endpoint);

            InternalEvent @event = new InternalEvent()
            {
                Type = InternalEvent.InternalEventType.Connect,
                Endpoint = endpoint
            };

            if (Config.TimeBasedConnectionChallenge)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Calculating with difficulty " + Config.ChallengeDifficulty);

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
                while ((hash << (sizeof(ulong) * 8 - Config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - Config.ChallengeDifficulty) != 0);

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
                int minSize = 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length + (Config.TimeBasedConnectionChallenge ? sizeof(ulong) * 3 : 0);

                // Calculate the actual size with respect to amplification padding
                int size = Math.Max(minSize, (int)Config.AmplificationPreventionHandshakePadding);

                // Allocate the memory
                HeapMemory memory = MemoryManager.AllocHeapMemory((uint)size);

                // Set the header
                memory.Buffer[0] = HeaderPacker.Pack(MessageType.ConnectionRequest);

                // Copy the identification token
                Buffer.BlockCopy(Constants.RUFFLES_PROTOCOL_IDENTIFICATION, 0, memory.Buffer, 1, Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length);

                if (Config.TimeBasedConnectionChallenge)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Using time based connection challenge. Writing solve with difficulty " + Config.ChallengeDifficulty);

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
                connection.Send(new ArraySegment<byte>(memory.Buffer, 0, (int)memory.VirtualCount), true);

                // Dealloc the memory
                MemoryManager.DeAlloc(memory);
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
            if (!Config.EnableQueuedIOEvents)
            {
                throw new InvalidOperationException("Cannot call DisconnectLater when EnableQueuedIOEvents is disabled");
            }

            if (connection != null && connection.State == ConnectionState.Connected)
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
        /// </summary>
        /// <param name="connection">The connection to disconnect.</param>
        /// <param name="sendMessage">If set to <c>true</c> the remote will be notified of the disconnect rather than timing out.</param>
        public bool DisconnectNow(Connection connection, bool sendMessage)
        {
            if (connection != null && connection.State == ConnectionState.Connected)
            {
                connection.Disconnect(sendMessage, false);

                return true;
            }

            return false;
        }

        private void StartNetworkLogic()
        {
            Stopwatch logicWatch = new Stopwatch();
            logicWatch.Start();

            while (IsRunning)
            {
                try
                {
                    if (Simulator != null)
                    {
                        Simulator.RunLoop();
                    }

                    if (Config.EnableQueuedIOEvents)
                    {
                        PollInternalIOQueue();
                    }

                    for (Connection connection = HeadConnection; connection != null; connection = connection.NextConnection)
                    {
                        connection.Update();
                    }

                    int sleepMs = (Config.LogicDelay - ((int)logicWatch.ElapsedMilliseconds));

                    logicWatch.Reset();
                    logicWatch.Start();

                    if (sleepMs > 0)
                    {
                        Thread.Sleep(sleepMs);
                    }
                }
                catch (Exception e)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when running internal loop: " + e);
                }
            }
        }

        private readonly EndPoint _fromIPv4Endpoint = new IPEndPoint(IPAddress.Any, 0);
        private readonly EndPoint _fromIPv6Endpoint = new IPEndPoint(IPAddress.IPv6Any, 0);

        private void StartReceiveLogic()
        {
            byte[] _incomingBuffer = new byte[Config.MaxBufferSize];
            List<Socket> _selectSockets = new List<Socket>();

            while (IsRunning)
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

                Socket.Select(_selectSockets, null, null, 1000 * 1000);

                for (int i = 0; i < _selectSockets.Count; i++)
                {
                    try
                    {
                        // Get a endpoint reference
                        EndPoint _endpoint = _selectSockets[i].AddressFamily == AddressFamily.InterNetwork ? _fromIPv4Endpoint : _selectSockets[i].AddressFamily == AddressFamily.InterNetworkV6 ? _fromIPv6Endpoint : null;

                        int size = _selectSockets[i].ReceiveFrom(_incomingBuffer, 0, _incomingBuffer.Length, SocketFlags.None, ref _endpoint);

                        HandlePacket(new ArraySegment<byte>(_incomingBuffer, 0, size), _endpoint, true);
                    }
                    catch (Exception e)
                    {
                        if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Error when receiving from socket: " + e);
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
                    @event.Connection.Disconnect(@event.SendMessage, false);
                }
                else if (@event.Type == InternalEvent.InternalEventType.Send)
                {
                    // Send the data
                    @event.Connection.HandleDelayedChannelSend(new ArraySegment<byte>(@event.Data.Buffer, (int)@event.Data.VirtualOffset, (int)@event.Data.VirtualCount), @event.ChannelId, @event.NoMerge);

                    // Dealloc the memory
                    MemoryManager.DeAlloc(@event.Data);
                }
            }
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
                MemoryManager = MemoryManager
            };
        }

        internal void SendRaw(EndPoint endpoint, ArraySegment<byte> payload)
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
            HeaderPacker.Unpack(payload.Array[payload.Offset], out MessageType messageType);

            if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Unpacked packet. [MessageType=" + (MessageType)messageType + "]");

            switch (messageType)
            {
                case MessageType.Merge:
                    {
                        if (!Config.EnablePacketMerging)
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

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
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
                case MessageType.ConnectionRequest:
                    {
                        if (payload.Count < Config.AmplificationPreventionHandshakePadding || payload.Count < 1 + Constants.RUFFLES_PROTOCOL_IDENTIFICATION.Length)
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

                        if (Config.TimeBasedConnectionChallenge)
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

                            if (secondsDiff > (long)Config.ConnectionChallengeTimeWindow || secondsDiff < -(long)Config.ConnectionChallengeTimeWindow)
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

                            // Save the IV to the sliding window
                            challengeInitializationVectors[userIv] = true;

                            // Calculate the hash the user claims have a collision
                            ulong claimedHash = HashProvider.GetStableHash64(challengeUnixTime, counter, userIv);

                            // Check if the hash collides
                            bool isCollided = ((claimedHash << (sizeof(ulong) * 8 - Config.ChallengeDifficulty)) >> (sizeof(ulong) * 8 - Config.ChallengeDifficulty)) == 0;

                            if (!isCollided)
                            {
                                // They failed the challenge
                                if (Logging.CurrentLogLevel <= LogLevel.Info) Logging.LogWarning("Client " + endpoint + " failed the connection request. They submitted an invalid answer. [ClaimedHash=" + claimedHash + "] [Counter=" + counter + "] [IV=" + userIv + "] [Time=" + challengeUnixTime + "]");
                                return;
                            }
                        }

                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogInfo("Client " + endpoint + " is being challenged");

                        Connection connection = AddNewConnection(endpoint, ConnectionState.RequestingChallenge);

                        if (connection != null)
                        {
                            // This connection was successfully added as pending

                            // Send connection request
                            connection.SendChallengeRequest();
                        }
                        else
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Error) Logging.LogError("Client " + endpoint + " could not be challenged. Allocation failed");
                        }
                    }
                    break;
                case MessageType.ChallengeRequest:
                    {
                        if (payload.Count < 10)
                        {
                            // The message is not large enough to contain all the data neccecary. Wierd server?
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Server " + endpoint + " sent us a payload that was too small. Disconnecting");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            ulong challenge = (((ulong)payload.Array[payload.Offset + 1 + 0]) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 1] << 8) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 2] << 16) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 3] << 24) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 4] << 32) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 5] << 40) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 6] << 48) |
                                                ((ulong)payload.Array[payload.Offset + 1 + 7] << 56));

                            byte difficulty = payload.Array[payload.Offset + 1 + sizeof(ulong)];

                            connection.HandleChallengeRequest(challenge, difficulty);
                        }
                    }
                    break;
                case MessageType.ChallengeResponse:
                    {
                        if (payload.Count < Config.AmplificationPreventionHandshakePadding)
                        {
                            // This message is too small. They might be trying to use us for amplification
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Client " + endpoint + " sent a challenge response that was smaller than the amplification padding");
                            return;
                        }

                        if (payload.Count < 9)
                        {
                            // The message is not large enough to contain all the data neccecary. Wierd client?
                            // TODO: Handle
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            ulong challengeResponse = (((ulong)payload.Array[payload.Offset + 1 + 0]) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 1] << 8) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 2] << 16) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 3] << 24) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 4] << 32) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 5] << 40) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 6] << 48) |
                                                        ((ulong)payload.Array[payload.Offset + 1 + 7] << 56));

                            connection.HandleChallengeResponse(challengeResponse);
                        }
                    }
                    break;
                case MessageType.Hail:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            if (payload.Count < 2)
                            {
                                // Invalid size.
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent a payload that was too small");
                                return;
                            }

                            // Read the amount of channels
                            byte channelCount = payload.Array[payload.Offset + 1];


                            if (channelCount > Constants.MAX_CHANNELS)
                            {
                                // Too many channels
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " more channels than allowed");
                                return;
                            }

                            if (payload.Count < channelCount + 2)
                            {
                                // Invalid size.
                                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent a payload that was too small");
                                return;
                            }

                            // Read the types and validate them (before alloc)
                            for (byte i = 0; i < channelCount; i++)
                            {
                                byte channelType = payload.Array[payload.Offset + 2 + i];

                                if (!Enum.IsDefined(typeof(ChannelType), channelType))
                                {
                                    // Unknown channel type. Disconnect.
                                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogError("Client " + endpoint + " sent an invalid ChannelType");
                                    return;
                                }
                            }

                            connection.HandleHail(new ArraySegment<byte>(payload.Array, payload.Offset + 2, payload.Array[payload.Offset + 1]));
                        }
                    }
                    break;
                case MessageType.HailConfirmed:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.HandleHailConfirmed();
                        }
                    }
                    break;
                case MessageType.Heartbeat:
                    {
                        if (!Config.EnableHeartbeats)
                        {
                            // TODO: Handle
                            // This is a missmatch.
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Heartbeat received from " + endpoint + " but the we do not have heartbeats enabled. Configuration missmatch?");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            // Heartbeats are sequenced to not properly handle network congestion
                            connection.HandleHeartbeat(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1));
                        }
                    }
                    break;
                case MessageType.Data:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.HandleChannelData(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1));
                        }
                    }
                    break;
                case MessageType.Ack:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.HandleChannelAck(new ArraySegment<byte>(payload.Array, payload.Offset + 1, payload.Count - 1));
                        }
                    }
                    break;
                case MessageType.Disconnect:
                    {
                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.Disconnect(false, false);
                        }
                    }
                    break;
                case MessageType.UnconnectedData:
                    {
                        if (!Config.AllowUnconnectedMessages)
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Got unconnected message but SocketConfig.AllowUnconnectedMessages is disabled.");
                            return;
                        }

                        // Alloc memory that can be borrowed to userspace
                        HeapMemory memory = MemoryManager.AllocHeapMemory((uint)payload.Count - 1);

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
                            MemoryManager = MemoryManager
                        });
                    }
                    break;
                case MessageType.MTURequest:
                    {
                        if (!Config.EnablePathMTU)
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Got MTURequest message but SocketConfig.EnablePathMTU is disabled.");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.HandleMTURequest((uint)payload.Count);
                        }
                    }
                    break;
                case MessageType.MTUResponse:
                    {
                        if (!Config.EnablePathMTU)
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Got MTUResponse message but SocketConfig.EnablePathMTU is disabled.");
                            return;
                        }

                        Connection connection = GetConnection(endpoint);

                        if (connection != null)
                        {
                            connection.HandleMTUResponse((uint)payload.Count);
                        }
                    }
                    break;
            }
        }

        internal Connection GetConnection(EndPoint endpoint)
        {
            // Lock to prevent grabbing a half dead connection
            connectionsLock.EnterReadLock();

            try
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
            finally
            {
                connectionsLock.ExitReadLock();
            }
        }

        internal void RemoveConnection(Connection connection)
        {
            // Lock when removing the connection to prevent another thread grabbing it before its fully dead.
            connectionsLock.EnterWriteLock();

            try
            {
                // Remove lookup
                if (addressConnectionLookup.Remove(connection.EndPoint))
                {
                    if (connection == HeadConnection)
                    {
                        HeadConnection = HeadConnection.NextConnection;
                    }

                    if (connection.PreviousConnection != null)
                    {
                        connection.PreviousConnection.NextConnection = connection.NextConnection;
                    }

                    if (connection.NextConnection != null)
                    {
                        connection.NextConnection.PreviousConnection = connection.PreviousConnection;
                    }

                    connection.PreviousConnection = null;
                }
            }
            finally
            {
                connectionsLock.ExitWriteLock();
            }
        }

        internal Connection AddNewConnection(EndPoint endpoint, ConnectionState state)
        {
            // Lock when adding connection to prevent grabbing a half alive connection.
            connectionsLock.EnterWriteLock();

            try
            {
                // Make sure they are not already connected to prevent an attack where a single person can fill all the slots.
                if (addressConnectionLookup.ContainsKey(endpoint))
                {
                    return null;
                }

                // Alloc on the heap
                Connection connection = new Connection(state, endpoint, this);

                // Add lookup
                addressConnectionLookup.Add(endpoint, connection);

                if (HeadConnection != null)
                {
                    // We have a connection as head.
                    connection.NextConnection = HeadConnection;
                    HeadConnection.PreviousConnection = connection;
                }

                HeadConnection = connection;

                return connection;
            }
            finally
            {
                connectionsLock.ExitWriteLock();
            }
        }
    }
}
