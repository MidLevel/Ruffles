using System;
using System.Threading;
using Ruffles.Collections;
using Ruffles.Configuration;
using Ruffles.Exceptions;
using Ruffles.Utils;

namespace Ruffles.Memory
{
    internal class MemoryManager
    {
        private int _createdHeapMemory = 0;
        private bool _hasWarnedAboutHeapMemoryLeaks = false;
        private readonly ConcurrentQueue<HeapMemory> _pooledHeapMemory = new ConcurrentQueue<HeapMemory>();

        private const uint minHeapMemorySize = 64;
        private const uint heapMemorySizeMultiple = 64;

        private int _createdPointerArrays = 0;
        private bool _hasWarnedAboutPointerArrayLeaks = false;
        private readonly ConcurrentQueue<HeapPointers> _pooledPointerArrays = new ConcurrentQueue<HeapPointers>();

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

        internal HeapPointers AllocHeapPointers(uint size)
        {
            uint allocSize = Math.Max(minPointerArraySize, CalculateMultiple(size, pointerArraySizeMultiple));

            bool pooled;

            if (!(pooled = _pooledPointerArrays.TryDequeue(out HeapPointers pointers)))
            {
                int createdHeapPointers = Interlocked.Increment(ref _createdPointerArrays);

                if (createdHeapPointers >= 1024 && !_hasWarnedAboutPointerArrayLeaks)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated. [HEAP POINTERS]");
                    _hasWarnedAboutPointerArrayLeaks = true;
                }

                pointers = new HeapPointers(allocSize);
            }

            pointers.EnsureSize(allocSize);

            pointers.isDead = false;
            pointers.VirtualCount = size;
            pointers.VirtualOffset = 0;

            if (pooled)
            {
                // If we got one from the pool, we need to clear it
                Array.Clear(pointers.Pointers, 0, pointers.Pointers.Length);
            }

#if DEBUG
            // The allocation stacktrace allows us to see where the alloc occured that caused the leak
            pointers.allocStacktrace = Environment.StackTrace;
#endif

            return pointers;
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
                Array.Clear(memory.Buffer, 0, memory.Buffer.Length);
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

        internal void DeAlloc(HeapPointers pointers)
        {
            if (pointers.isDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            pointers.isDead = true;
            pointers.VirtualOffset = 0;
            pointers.VirtualCount = 0;

            _pooledPointerArrays.Enqueue(pointers);
        }
    }
}
