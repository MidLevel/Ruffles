using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Configuration;

namespace Ruffles.Core
{
    /// <summary>
    /// A socket manager that can run Ruffles in a threaded mode.
    /// </summary>
    public class SocketManager
    {
        private readonly List<RuffleSocket> _sockets = new List<RuffleSocket>();
        private Thread _thread;

        /// <summary>
        /// The thread lock that has to be grabbed to modify the underlying connections when running in threaded mode.
        /// Modifications includes all connection APIs.
        /// </summary>
        /// <value>The object to lock.</value>
        public object ThreadLock { get; } = new object();
        /// <summary>
        /// Gets a value indicating whether this <see cref="T:Ruffles.Core.RufflesManager"/> is threaded.
        /// </summary>
        /// <value><c>true</c> if is threaded; otherwise, <c>false</c>.</value>
        public bool IsThreaded { get; }
        /// <summary>
        /// Gets a value indicating whether this <see cref="T:Ruffles.Core.RufflesManager"/> is running.
        /// </summary>
        /// <value><c>true</c> if is running; otherwise, <c>false</c>.</value>
        public bool IsRunning { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Ruffles.Core.RufflesManager"/> class.
        /// </summary>
        /// <param name="threaded">If set to <c>true</c> threaded.</param>
        public SocketManager(bool threaded = true)
        {
            this.IsThreaded = threaded;
        }

        /// <summary>
        /// Init this manager instance.
        /// </summary>
        public void Init()
        {
            if (IsRunning)
            {
                throw new InvalidOperationException("Manager is already started");
            }

            IsRunning = true;

            if (IsThreaded)
            {
                _thread = new Thread(() =>
                {
                    while (IsRunning)
                    {
                        RunAllInternals();
                    }
                });

                _thread.Start();
            }
        }

        /// <summary>
        /// Stops this instance.
        /// </summary>
        public void Shutdown()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            IsRunning = false;

            if (IsThreaded)
            {
                _thread.Join();
            }
        }

        private void RunAllInternals()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            for (int i = 0; i < _sockets.Count; i++)
            {
                if (IsThreaded)
                {
                    lock (ThreadLock)
                    {
                        _sockets[i].RunInternalLoop();
                    }
                }
                else
                {
                    _sockets[i].RunInternalLoop();
                }
            }
        }

        /// <summary>
        /// Adds a local socket to the manager.
        /// </summary>
        /// <returns>The local socket.</returns>
        /// <param name="config">The socket configuration.</param>
        public RuffleSocket AddSocket(SocketConfig config)
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            RuffleSocket socket = new RuffleSocket(config);

            if (IsThreaded)
            {
                lock (ThreadLock)
                {
                    _sockets.Add(socket);
                }
            }
            else
            {
                _sockets.Add(socket);
            }

            return socket;
        }

        /// <summary>
        /// Runs all the internals. Only usable when not using a threaded manager.
        /// Calling this is not neccecary, the PollAllSockets will do it for you.
        /// </summary>
        public void RunInternals()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            if (IsThreaded)
            {
                throw new InvalidOperationException("Cannot run the internals when using a threaded manager");
            }

            RunAllInternals();
        }


        /// <summary>
        /// Runs all the internals and polls all the sockets for events.
        /// </summary>
        /// <returns>The first event.</returns>
        public NetworkEvent PollAllSockets()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            if (!IsThreaded)
            {
                RunInternals();
            }

            if (IsThreaded)
            {
                lock (ThreadLock)
                {
                    for (int i = 0; i < _sockets.Count; i++)
                    {
                        NetworkEvent @event = _sockets[i].Poll();

                        if (@event.Type != NetworkEventType.Nothing)
                        {
                            return @event;
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < _sockets.Count; i++)
                {
                    NetworkEvent @event = _sockets[i].Poll();

                    if (@event.Type != NetworkEventType.Nothing)
                    {
                        return @event;
                    }
                }
            }

            return new NetworkEvent()
            {
                Connection = null,
                Socket = null,
                Data = new ArraySegment<byte>(),
                AllowUserRecycle = false,
                InternalMemory = null,
                Type = NetworkEventType.Nothing
            };
        }
    }
}
