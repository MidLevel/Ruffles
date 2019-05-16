using System;
using System.Collections.Generic;
using Ruffles.Utils;

namespace Ruffles.Messaging
{
    internal class SlidingSet<T>
    {
        private readonly Dictionary<T, ushort> _indexLookup = new Dictionary<T, ushort>();
        private readonly ushort[] _indexes;
        private readonly T[] _array;
        private readonly byte _wrapSize = sizeof(ushort);
        private readonly bool _resetOld;
        private ushort _lastHighestSequence;
        private ushort _indexCounter;

        public SlidingSet(int size, bool resetOld)
        {
            if (resetOld && size % 8 != 0)
            {
                throw new ArgumentException("Size needs to be a multiple of 8 when resetOld is enabled");
            }

            _array = new T[size];
            _indexes = new ushort[size];
            _resetOld = resetOld;
        }

        public bool this[T key]
        {
            get
            {
                if (_indexLookup.ContainsKey(key))
                {
                    int index = _indexLookup[key];

                    int arrayIndex = NumberUtils.WrapMod(index, _indexes.Length);

                    if (_indexes[arrayIndex] == index)
                        return true;
                }

                return false;
            }
            set
            {
                ushort index;

                if (_indexLookup.ContainsKey(key))
                {
                    index = _indexLookup[key];
                }
                else
                {
                    index = _indexCounter;
                    _indexCounter++;
                }

                if (_resetOld)
                {
                    long distance = SequencingUtils.Distance(index, _lastHighestSequence, _wrapSize);

                    if (distance > 0)
                    {
                        for (ushort i = 1; i < distance; i++)
                        {
                            int resetArrayIndex = NumberUtils.WrapMod((_lastHighestSequence + index + i), _indexes.Length);
                            _indexes[resetArrayIndex] = (ushort)(_lastHighestSequence + index + i);

                            if (_indexLookup.ContainsKey(_array[resetArrayIndex]))
                                _indexLookup.Remove(_array[resetArrayIndex]);

                            _array[resetArrayIndex] = default(T);
                        }

                        _lastHighestSequence = index;
                    }
                }

                int arrayIndex = NumberUtils.WrapMod(index, _indexes.Length);

                if (_indexLookup.ContainsKey(_array[arrayIndex]))
                    _indexLookup.Remove(_array[arrayIndex]);

                if (value)
                    _indexLookup.Add(key, index);

                _indexes[arrayIndex] = index;
                _array[arrayIndex] = key;
            }
        }

        public void Release()
        {
            for (int i = 0; i < _array.Length; i++)
            {
                _indexes[i] = 0;
                _array[i] = default(T);
            }

            _lastHighestSequence = 0;
        }
    }
}
