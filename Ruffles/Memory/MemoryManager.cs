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
        private readonly ConcurrentCircularQueue<HeapMemory> _pooledHeapMemory;

        private const uint minHeapMemorySize = 64;
        private const uint heapMemorySizeMultiple = 64;

        private int _createdPointerArrays = 0;
        private bool _hasWarnedAboutPointerArrayLeaks = false;
        private readonly ConcurrentCircularQueue<HeapPointers> _pooledPointerArrays;

        private const uint minPointerArraySize = 64;
        private const uint pointerArraySizeMultiple = 64;

        private int _createdMemoryWrappers = 0;
        private bool _hasWarnedAboutMemoryWrapperLeaks = false;
        private readonly ConcurrentCircularQueue<MemoryWrapper> _pooledMemoryWrappers;

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
            _pooledHeapMemory = new ConcurrentCircularQueue<HeapMemory>(_configuration.MemoryManagerMaxHeapMemory);
            _pooledPointerArrays = new ConcurrentCircularQueue<HeapPointers>(_configuration.MemoryManagerMaxHeapPointers);
            _pooledMemoryWrappers = new ConcurrentCircularQueue<MemoryWrapper>(_configuration.MemoryManagerMaxMemoryWrappers);
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

            pointers.IsDead = false;

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

            memory.IsDead = false;

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

        internal MemoryWrapper AllocMemoryWrapper(HeapMemory allocatedMemory)
        {
            return AllocMemoryWrapper(allocatedMemory, null);
        }

        internal MemoryWrapper AllocMemoryWrapper(ArraySegment<byte>? directMemory)
        {
            return AllocMemoryWrapper(null, directMemory);
        }

        private MemoryWrapper AllocMemoryWrapper(HeapMemory allocatedMemory, ArraySegment<byte>? directMemory)
        {
            bool pooled;

            if (!(pooled = _pooledMemoryWrappers.TryDequeue(out MemoryWrapper wrapper)))
            {
                int createdMemoryWrappers = Interlocked.Increment(ref _createdMemoryWrappers);

                if (createdMemoryWrappers >= 1024 && !_hasWarnedAboutMemoryWrapperLeaks)
                {
                    if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated. [MEMORY WRAPPER]");
                    _hasWarnedAboutMemoryWrapperLeaks = true;
                }

                wrapper = new MemoryWrapper();
            }

            wrapper.IsDead = false;

            wrapper.AllocatedMemory = allocatedMemory;
            wrapper.DirectMemory = directMemory;

#if DEBUG
            // The allocation stacktrace allows us to see where the alloc occured that caused the leak
            wrapper.allocStacktrace = Environment.StackTrace;
#endif

            return wrapper;
        }

        internal void DeAlloc(HeapMemory memory)
        {
            if (memory.IsDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            memory.VirtualOffset = 0;
            memory.VirtualCount = 0;

            memory.IsDead = true;

            if (!_pooledHeapMemory.TryEnqueue(memory))
            {
                // Failed to enqueue memory. Queue is full
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Could not return heap memory. The queue is full. The memory will be given to the garbage collector. [HEAP MEMORY]");
            }
        }

        internal void DeAlloc(HeapPointers pointers)
        {
            if (pointers.IsDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            pointers.VirtualOffset = 0;
            pointers.VirtualCount = 0;

            pointers.IsDead = true;

            if (!_pooledPointerArrays.TryEnqueue(pointers))
            {
                // Failed to enqueue pointers. Queue is full
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Could not return heap pointers. The queue is full. The memory will be given to the garbage collector. [HEAP POINTERS]");
            }
        }

        internal void DeAlloc(MemoryWrapper wrapper)
        {
            if (wrapper.IsDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            wrapper.AllocatedMemory = null;
            wrapper.DirectMemory = null;

            wrapper.IsDead = true;

            if (!_pooledMemoryWrappers.TryEnqueue(wrapper))
            {
                // Failed to enqueue pointers. Queue is full
                if (Logging.CurrentLogLevel <= LogLevel.Warning) Logging.LogWarning("Could not return memory wrapper. The queue is full. The memory will be given to the garbage collector. [MEMORY WRAPPER]");
            }
        }
    }
}
