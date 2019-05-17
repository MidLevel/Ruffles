using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Configuration;

namespace Ruffles.Core
{
    public class RufflesManager
    {
        private readonly List<Listener> _listeners = new List<Listener>();
        private Thread _thread;

        public object ThreadLock { get; } = new object();
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

            for (int i = 0; i < _listeners.Count; i++)
            {
                if (IsThreaded)
                {
                    lock (ThreadLock)
                    {
                        _listeners[i].RunInternalLoop();
                    }
                }
                else
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

            Listener listener = new Listener(config);

            if (IsThreaded)
            {
                lock (ThreadLock)
                {
                    _listeners.Add(listener);
                }
            }
            else
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

            if (IsThreaded)
            {
                lock (ThreadLock)
                {
                    for (int i = 0; i < _listeners.Count; i++)
                    {
                        NetworkEvent @event = _listeners[i].Poll();

                        if (@event.Type != NetworkEventType.Nothing)
                        {
                            return @event;
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < _listeners.Count; i++)
                {
                    NetworkEvent @event = _listeners[i].Poll();

                    if (@event.Type != NetworkEventType.Nothing)
                    {
                        return @event;
                    }
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
