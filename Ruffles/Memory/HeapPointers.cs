using System;
using Ruffles.Exceptions;
using Ruffles.Utils;

namespace Ruffles.Memory
{
    internal class HeapPointers
    {
        public object[] Pointers
        {
            get
            {
                if (isDead)
                {
                    throw new MemoryException("Cannot access dead memory");
                }

                return _pointers;
            }
        }

        public uint VirtualOffset { get; set; }
        public uint VirtualCount { get; set; }

        private object[] _pointers;
        internal bool isDead;

#if DEBUG
        internal string allocStacktrace;
#endif

        public HeapPointers(uint size)
        {
            _pointers = new object[size];
            VirtualOffset = 0;
            VirtualCount = size;
        }

        public void EnsureSize(uint size)
        {
            if (_pointers.Length < size)
            {
                object[] oldBuffer = _pointers;

                _pointers = new object[size];

                Array.Copy(oldBuffer, 0, _pointers, 0, oldBuffer.Length);
            }
        }

        ~HeapPointers()
        {
            try
            {
                // If shutdown of the CLR has started, or the application domain is being unloaded. We don't want to print leak warnings. As these are legitimate deallocs and not leaks.
                if (!Environment.HasShutdownStarted)
                {
                    if (!isDead)
                    {
#if DEBUG
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Pointers was just leaked from the MemoryManager [Size=" + Pointers.Length + "] AllocStack: " + allocStacktrace);
#else
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Pointers was just leaked from the MemoryManager [Size=" + Pointers.Length + "]");

#endif
                    }
                    else
                    {
#if DEBUG
                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogWarning("Dead pointers was just leaked from the MemoryManager [Size=" + _pointers.Length + "] AllocStack: " + allocStacktrace);
#else
                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogWarning("Dead pointers was just leaked from the MemoryManager [Size=" + _pointers.Length + "]");
#endif
                    }
                }
            }
            catch
            {
                // Supress
            }
        }
    }
}
