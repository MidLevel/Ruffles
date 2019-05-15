using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Configuration;

namespace Ruffles.Core
{
    public class RufflesManager
    {
        private readonly bool Threaded = true;
        private readonly Thread _thread;

        private readonly List<Listener> _listeners = new List<Listener>();
        private readonly object _listenersLock = new object();


        public RufflesManager(bool threaded = true)
        {
            this.Threaded = threaded;

            if (Threaded)
            {
                _thread = new Thread(() =>
                {
                    while (true)
                    {
                        RunAllInternals();
                        Thread.Sleep(1);
                    }
                });
                _thread.Start();
            }
        }

        private void RunAllInternals()
        {
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
            if (Threaded && !config.EnableThreadSafety)
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
            if (Threaded)
            {
                throw new InvalidOperationException("Cannot run the internals when using a threaded manager");
            }

            RunAllInternals();
        }

        public NetworkEvent PollAllListeners()
        {
            if (!Threaded)
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
