using System;
using Ruffles.Memory;
using Ruffles.Utils;

namespace Ruffles.Collections
{
    internal class HeapableFastWindow<T> where T : IMemoryReleasable
    {
        private struct Element
        {
            public int Index;
            public T Value;
        }

        private readonly Element[] _array;

        private readonly MemoryManager _memoryManager;

        public HeapableFastWindow(int size, MemoryManager memoryManager)
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

            _memoryManager = memoryManager;
        }

        public bool CanSet(int index)
        {
            bool foundEmpty = false;

            for (int i = 0; i < _array.Length; i++)
            {
                foundEmpty |= _array[NumberUtils.WrapMod(index + i, _array.Length)].Index == -1;

                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
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
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    return true;
                }
            }

            return false;
        }

        public bool CanUpdateOrSet(int index)
        {
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index || _array[NumberUtils.WrapMod(index + i, _array.Length)].Index == -1)
                {
                    return true;
                }
            }

            return false;
        }

        public bool TryUpdateOrSet(int index, T value)
        {
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    _array[NumberUtils.WrapMod(index + i, _array.Length)] = new Element()
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
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == -1)
                {
                    _array[NumberUtils.WrapMod(index + i, _array.Length)] = new Element()
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
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    throw new ArgumentOutOfRangeException(nameof(index), index, "Cannot set a when value already exists");
                }
            }

            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == -1)
                {
                    _array[NumberUtils.WrapMod(index + i, _array.Length)] = new Element()
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
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    _array[NumberUtils.WrapMod(index + i, _array.Length)] = new Element()
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
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    _array[NumberUtils.WrapMod(index + i, _array.Length)] = new Element()
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
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    return true;
                }
            }

            return false;
        }

        public bool TryGet(int index, out T value)
        {
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[NumberUtils.WrapMod(index + i, _array.Length)].Index == index)
                {
                    value = _array[NumberUtils.WrapMod(index + i, _array.Length)].Value;

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
                if (_array[i].Value != null && _array[i].Value.IsAlloced)
                {
                    _array[i].Value.DeAlloc(_memoryManager);
                }

                _array[i] = new Element()
                {
                    Index = -1,
                    Value = default(T)
                };
            }
        }
    }
}
