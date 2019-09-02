using System;
using System.Collections.Generic;
using Ruffles.Exceptions;

namespace Ruffles.Memory
{
    internal static class MemoryManager
    {
        private static ushort _createdPools = 0;
        private static bool _hasWarnedAboutLeak = false;
        private static readonly Queue<HeapMemory> _pooledMemory = new Queue<HeapMemory>();

        private const uint minBufferSize = 64;
        private const uint bufferMultiple = 64;

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

        internal static HeapMemory Alloc(uint size)
        {
            uint allocSize = Math.Max(minBufferSize, CalculateMultiple(size, bufferMultiple));

            if (_pooledMemory.Count == 0)
            {
                _createdPools++;

                if (_createdPools >= 1024 && !_hasWarnedAboutLeak)
                {
                    Console.WriteLine("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated.");
                    _hasWarnedAboutLeak = true;
                }

                HeapMemory memory = new HeapMemory(allocSize);

                memory.isDead = false;
                memory.VirtualCount = size;
                memory.VirtualOffset = 0;

                return memory;
            }
            else
            {
                HeapMemory memory = _pooledMemory.Dequeue();

                memory.EnsureSize(allocSize);

                memory.isDead = false;
                memory.VirtualCount = size;
                memory.VirtualOffset = 0;

                return memory;
            }
        }

        internal static void DeAlloc(HeapMemory memory)
        {
            if (memory.isDead)
            {
                throw new MemoryException("Cannot deallocate already dead memory");
            }

            memory.isDead = true;
            memory.VirtualOffset = 0;
            memory.VirtualCount = 0;

            _pooledMemory.Enqueue(memory);
        }
    }
}
