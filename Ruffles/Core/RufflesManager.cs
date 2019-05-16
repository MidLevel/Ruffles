using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Configuration;

namespace Ruffles.Core
{
    public class RufflesManager
    {
        private readonly List<Listener> _listeners = new List<Listener>();
        private readonly object _listenersLock = new object();

        private Thread _thread = null;

        public bool IsThreaded { get; }
        public bool IsRunning { get; private set; }

        public RufflesManager(bool threaded = true)
        {
            this.IsThreaded = threaded;
        }

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

            lock (_listenersLock)
            {
                for (int i = 0; i < _listeners.Count; i++)
                {
                    _listeners[i].RunInternalLoop();
                }
            }
        }

        public Listener AddListener(ListenerConfig config)
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            if (IsThreaded && !config.EnableThreadSafety)
            {
                Console.WriteLine("Running a threaded manager without thread safety on the Listener is not recomended!");
            }

            Listener listener = new Listener(config);

            lock (_listenersLock)
            {
                _listeners.Add(listener);
            }

            return listener;
        }

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

        public NetworkEvent PollAllListeners()
        {
            if (!IsRunning)
            {
                throw new InvalidOperationException("Manager is not started");
            }

            if (!IsThreaded)
            {
                RunInternals();
            }

            for (int i = 0; i < _listeners.Count; i++)
            {
                NetworkEvent @event = _listeners[i].Poll();

                if (@event.Type != NetworkEventType.Nothing)
                {
                    return @event;
                }
            }

            return new NetworkEvent()
            {
                Connection = null,
                Listener = null,
                Data = new ArraySegment<byte>(),
                AllowUserRecycle = false,
                InternalMemory = null,
                Type = NetworkEventType.Nothing
            };
        }
    }
}
