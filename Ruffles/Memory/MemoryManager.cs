using System;
using System.Collections.Generic;
using System.Threading;
using Ruffles.Exceptions;

namespace Ruffles.Memory
{
    internal static class MemoryManager
    {
        private static ushort _createdPools = 0;
        private static bool _hasWarnedAboutLeak = false;
        private static readonly Queue<HeapMemory> _pooledMemory = new Queue<HeapMemory>();
        private static readonly ReaderWriterLockSlim _poolLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        internal static HeapMemory Alloc(int size)
        {
            _poolLock.EnterWriteLock();
            try
            {
                if (_pooledMemory.Count == 0)
                {
                    _createdPools++;

                    if (_createdPools >= 1024 && !_hasWarnedAboutLeak)
                    {
                        Console.WriteLine("Memory leak detected. Are you leaking memory to the GC or are your windows too large? Leaking memory to the GC will cause slowdowns. Make sure all memory is deallocated.");
                        _hasWarnedAboutLeak = true;
                    }

                    return new HeapMemory(size);
                }

                HeapMemory memory = _pooledMemory.Dequeue();

                memory.EnsureSize(size);

                memory.isDead = false;
                memory.VirtualCount = size;
                memory.VirtualOffset = 0;

                return memory;
            }
            finally
            {
                _poolLock.ExitWriteLock();
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

            _poolLock.EnterWriteLock();
            try
            {
                _pooledMemory.Enqueue(memory);
            }
            finally
            {
                _poolLock.ExitWriteLock();
            }
        }
    }
}
