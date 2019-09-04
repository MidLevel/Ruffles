using System;
using System.Collections.Generic;
using Ruffles.Configuration;
using Ruffles.Exceptions;
using Ruffles.Utils;

namespace Ruffles.Memory
{
    internal class MemoryManager : IDisposable
    {
        private ushort _createdHeapMemory = 0;
        private bool _hasWarnedAboutHeapMemoryLeaks = false;
        private readonly Queue<HeapMemory> _pooledHeapMemory = new Queue<HeapMemory>();

        private const uint minHeapMemorySize = 64;
        private const uint heapMemorySizeMultiple = 64;

        private ushort _createdPointerArrays = 0;
        private bool _hasWarnedAboutPointerArrayLeaks = false;
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

        internal HeapMemory AllocHeapMemory(uint size)
        {
            uint allocSize = Math.Max(minHeapMemorySize, CalculateMultiple(size, heapMemorySizeMultiple));

            if (_pooledHeapMemory.Count == 0)
            {
                _createdHeapMemory++;

                if (_createdHeapMemory >= 1024 && !_hasWarnedAboutHeapMemoryLeaks)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated. [HEAP MEMORY]");
                    _hasWarnedAboutHeapMemoryLeaks = true;
                }

                HeapMemory memory = new HeapMemory(allocSize);

                memory.isDead = false;
                memory.VirtualCount = size;
                memory.VirtualOffset = 0;

                return memory;
            }
            else
            {
                HeapMemory memory = _pooledHeapMemory.Dequeue();

                memory.EnsureSize(allocSize);

                memory.isDead = false;
                memory.VirtualCount = size;
                memory.VirtualOffset = 0;

                return memory;
            }
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
                _pooledPointerArrays.Add(pointers);
            }
        }

        public void Dispose()
        {
            _pooledPointerArrays.Clear();
            _pooledPointerArrays.Clear();
        }
    }
}
