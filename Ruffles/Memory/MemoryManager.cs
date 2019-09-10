using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Collections;
using Ruffles.Configuration;
using Ruffles.Exceptions;
using Ruffles.Utils;

namespace Ruffles.Memory
{
    internal class MemoryManager : IDisposable
    {
        private int _createdHeapMemory = 0;
        private bool _hasWarnedAboutHeapMemoryLeaks = false;
        private readonly ConcurrentQueue<HeapMemory> _pooledHeapMemory = new ConcurrentQueue<HeapMemory>();

        private const uint minHeapMemorySize = 64;
        private const uint heapMemorySizeMultiple = 64;

        private int _createdPointerArrays = 0;
        private bool _hasWarnedAboutPointerArrayLeaks = false;
        private readonly object _pointerLock = new object();
        private readonly List<object[]> _pooledPointerArrays = new List<object[]>();

        private const uint minPointerArraySize = 64;
        private const uint pointerArraySizeMultiple = 64;

        private readonly SocketConfig _configuration;

        internal static uint CalculateMultiple(uint minSize, uint multiple)
        {
            uint remainder = minSize % multiple;

            uint result = minSize - remainder;

            if (remainder > (multiple / 2))
                result += multiple;

            if (result < minSize)
                result += multiple;

            return result;
        }


        internal MemoryManager(SocketConfig config)
        {
            _configuration = config;
        }

        internal object[] AllocPointers(uint size)
        {
            uint allocSize = Math.Max(minPointerArraySize, CalculateMultiple(size, pointerArraySizeMultiple));

            if (!_configuration.PoolPointerArrays)
            {
                return new object[allocSize];
            }
            else
            {
                lock (_pointerLock)
                {
                    object[] pointers = null;

                    if (_configuration.PoolPointerArrays)
                    {
                        for (int i = 0; i < _pooledPointerArrays.Count; i++)
                        {
                            if (_pooledPointerArrays[i].Length >= size)
                            {
                                pointers = _pooledPointerArrays[i];
                                _pooledPointerArrays.RemoveAt(i);
                                break;
                            }
                        }
                    }

                    if (pointers == null)
                    {
                        _createdPointerArrays++;

                        if (_createdPointerArrays >= 1024 && !_hasWarnedAboutPointerArrayLeaks)
                        {
                            if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated. [POINTERS ARRAYS]");
                            _hasWarnedAboutPointerArrayLeaks = true;
                        }

                        if (_pooledPointerArrays.Count > 0)
                        {
                            // Delete this one for expansion.
                            _pooledPointerArrays.RemoveAt(0);
                        }

                        pointers = new object[allocSize];
                    }
                    else
                    {
                        Array.Clear(pointers, 0, pointers.Length);
                    }

                    return pointers;
                }
            }
        }

        internal HeapMemory AllocHeapMemory(uint size)
        {
            uint allocSize = Math.Max(minHeapMemorySize, CalculateMultiple(size, heapMemorySizeMultiple));

            bool pooled;

            if (!(pooled = _pooledHeapMemory.TryDequeue(out HeapMemory memory)))
            {
                int createdHeapMemory = Interlocked.Increment(ref _createdHeapMemory);

                if (createdHeapMemory >= 1024 && !_hasWarnedAboutHeapMemoryLeaks)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated. [HEAP MEMORY]");
                    _hasWarnedAboutHeapMemoryLeaks = true;
                }

                memory = new HeapMemory(allocSize);
            }

            memory.EnsureSize(allocSize);

            memory.isDead = false;
            memory.VirtualCount = size;
            memory.VirtualOffset = 0;

            if (pooled)
            {
                // If we got one from the pool, we need to clear it
                Array.Clear(memory.Buffer, 0, (int)size);
            }

#if DEBUG
            // The allocation stacktrace allows us to see where the alloc occured that caused the leak
            memory.allocStacktrace = Environment.StackTrace;
#endif

            return memory;
        }

        internal void DeAlloc(HeapMemory memory)
        {
            if (memory.isDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            memory.isDead = true;
            memory.VirtualOffset = 0;
            memory.VirtualCount = 0;

            _pooledHeapMemory.Enqueue(memory);
        }

        internal void DeAlloc(object[] pointers)
        {
            if (_configuration.PoolPointerArrays)
            {
                lock (_pointerLock)
                {
                    _pooledPointerArrays.Add(pointers);
                }
            }
        }

        public void Dispose()
        {
            lock (_pointerLock)
            {
                _pooledPointerArrays.Clear();
            }
        }
    }
}
