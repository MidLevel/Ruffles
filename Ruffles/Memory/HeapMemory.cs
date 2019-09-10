using System;
using Ruffles.Exceptions;
using Ruffles.Utils;

namespace Ruffles.Memory
{
    internal class HeapMemory
    {
        public byte[] Buffer
        {
            get
            {
                if (isDead)
                {
                    throw new MemoryException("Cannot access dead memory");
                }

                return _buffer;
            }
        }

        public uint VirtualOffset { get; set; }
        public uint VirtualCount { get; set; }

        private byte[] _buffer;
        internal bool isDead;

#if DEBUG
        internal string allocStacktrace;
#endif

        public HeapMemory(uint size)
        {
            _buffer = new byte[size];
            VirtualOffset = 0;
            VirtualCount = size;
        }

        public void EnsureSize(uint size)
        {
            if (_buffer.Length < size)
            {
                byte[] oldBuffer = _buffer;

                _buffer = new byte[size];

                System.Buffer.BlockCopy(oldBuffer, 0, _buffer, 0, oldBuffer.Length);
            }
        }

        ~HeapMemory()
        {
            try
            {
                // If shutdown of the CLR has started, or the application domain is being unloaded. We don't want to print leak warnings. As these are legitimate deallocs and not leaks.
                if (!Environment.HasShutdownStarted)
                {
                    if (!isDead)
                    {
#if DEBUG
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory was just leaked from the MemoryManager [Size=" + Buffer.Length + "] AllocStack: " + allocStacktrace);
#else
                        if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory was just leaked from the MemoryManager [Size=" + Buffer.Length + "]");

#endif
                    }
                    else
                    {
#if DEBUG
                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogWarning("Dead memory was just leaked from the MemoryManager [Size=" + _buffer.Length + "] AllocStack: " + allocStacktrace);
#else
                        if (Logging.CurrentLogLevel <= LogLevel.Debug) Logging.LogWarning("Dead memory was just leaked from the MemoryManager [Size=" + _buffer.Length + "]");
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
