using System;
using Ruffles.Memory;
using Ruffles.Utils;

namespace Ruffles.Messaging
{
    internal class HeapableSlidingWindow<T> where T : IMemoryReleasable
    {
        private readonly int[] _indexes;
        private readonly T[] _array;
        private readonly byte _wrapSize;
        private readonly bool _resetOld;
        private ulong _lastHighestSequence;

        private readonly MemoryManager _memoryManager;

        public HeapableSlidingWindow(int size, bool resetOld, byte wrapSize, MemoryManager memoryManager)
        {
            if (resetOld && size % 8 != 0)
            {
                throw new ArgumentException("Size needs to be a multiple of 8 when resetOld is enabled");
            }

            _array = new T[size];
            _indexes = new int[size];
            _wrapSize = wrapSize;
            _resetOld = resetOld;

            _memoryManager = memoryManager;
        }

        public T this[int index]
        {
            get
            {
                int arrayIndex = NumberUtils.WrapMod(index, _array.Length);

                if (_indexes[arrayIndex] == index)
                    return _array[arrayIndex];
                else
                    return default(T);
            }
            set
            {
                if (_resetOld)
                {
                    long distance = SequencingUtils.Distance((ulong)index, _lastHighestSequence, _wrapSize);

                    if (distance > 0)
                    {
                        for (int i = 1; i < distance; i++)
                        {
                            int resetArrayIndex = NumberUtils.WrapMod(((int) _lastHighestSequence + index + i), _array.Length);
                            _indexes[resetArrayIndex] = ((int) _lastHighestSequence + index + i);
                            _array[resetArrayIndex] = default(T);
                        }

                        _lastHighestSequence = (ulong)index;
                    }
                }

                int arrayIndex = NumberUtils.WrapMod(index, _array.Length);
                _indexes[arrayIndex] = index;
                _array[arrayIndex] = value;
            }
        }

        public void Release()
        {
            for (int i = 0; i < _array.Length; i++)
            {
                _indexes[i] = 0;
                _array[i].DeAlloc(_memoryManager);
                _array[i] = default(T);
            }

            _lastHighestSequence = 0;
        }
    }
}
