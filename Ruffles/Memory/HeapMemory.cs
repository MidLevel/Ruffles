using Ruffles.Exceptions;

namespace Ruffles.Memory
{
    internal class HeapMemory
    {
        public byte[] Buffer
        {
            get
            {
                if (isDead)
                {
                    throw new MemoryException("Cannot access dead memory");
                }

                return _buffer;
            }
        }

        public int VirtualOffset { get; set; }
        public int VirtualCount { get; set; }

        private byte[] _buffer;
        internal bool isDead;

        public HeapMemory(int size)
        {
            _buffer = new byte[size];
            VirtualOffset = 0;
            VirtualCount = size;
        }

        public void EnsureSize(int size)
        {
            if (_buffer.Length < size)
            {
                byte[] oldBuffer = _buffer;

                _buffer = new byte[size];

                System.Buffer.BlockCopy(oldBuffer, 0, _buffer, 0, oldBuffer.Length);
            }
        }
    }
}
