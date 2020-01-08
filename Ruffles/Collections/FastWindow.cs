using System;
using Ruffles.Memory;
using Ruffles.Utils;

namespace Ruffles.Collections
{
    internal class FastWindow<T>
    {
        private struct Element
        {
            public int Index;
            public T Value;
        }

        private readonly Element[] _array;

        public FastWindow(int size)
        {
            _array = new Element[size];

            for (int i = 0; i < _array.Length; i++)
            {
                _array[i] = new Element()
                {
                    Index = -1,
                    Value = default(T)
                };
            }
        }

        public bool CanSet(int index)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            bool foundEmpty = false;

            for (int i = 0; i < _array.Length; i++)
            {
                foundEmpty |= _array[arrayBaseIndex + i].Index == -1;

                if (_array[arrayBaseIndex + i].Index == index)
                {
                    return false;
                }
            }

            return foundEmpty;
        }

        public bool CanUpdate(int index)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    return true;
                }
            }

            return false;
        }

        public bool CanUpdateOrSet(int index)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index || _array[arrayBaseIndex + i].Index == -1)
                {
                    return true;
                }
            }

            return false;
        }

        public bool TryUpdateOrSet(int index, T value)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    _array[arrayBaseIndex + i] = new Element()
                    {
                        Index = index,
                        Value = value
                    };

                    return true;
                }
            }

            // If we have not yet set. Check if there is a new spot instead
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == -1)
                {
                    _array[arrayBaseIndex + i] = new Element()
                    {
                        Index = index,
                        Value = value
                    };

                    return true;
                }
            }

            return false;
        }

        public bool TrySet(int index, T value)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    throw new ArgumentOutOfRangeException(nameof(index), index, "Cannot set a when value already exists");
                }
            }

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == -1)
                {
                    _array[arrayBaseIndex + i] = new Element()
                    {
                        Index = index,
                        Value = value
                    };

                    return true;
                }
            }

            return false;
        }

        public bool TryUpdate(int index, T value)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    _array[arrayBaseIndex + i] = new Element()
                    {
                        Index = index,
                        Value = value
                    };

                    return true;
                }
            }

            return false;
        }

        public bool TryRemove(int index)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    _array[arrayBaseIndex + i] = new Element()
                    {
                        Index = -1,
                        Value = default(T)
                    };

                    return true;
                }
            }

            return false;
        }

        public bool Contains(int index)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    return true;
                }
            }

            return false;
        }

        public bool TryGet(int index, out T value)
        {
            int arrayBaseIndex = NumberUtils.WrapMod(index, _array.Length);

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[arrayBaseIndex + i].Index == index)
                {
                    value = _array[arrayBaseIndex + i].Value;

                    return true;
                }
            }

            value = default(T);
            return false;
        }

        public void Release()
        {
            for (int i = 0; i < _array.Length; i++)
            {
                _array[i] = new Element()
                {
                    Index = -1,
                    Value = default(T)
                };
            }
        }
    }
}
