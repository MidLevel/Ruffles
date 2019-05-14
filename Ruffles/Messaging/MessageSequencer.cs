using Ruffles.Memory;

namespace Ruffles.Messaging
{
    internal class MessageSequencer<T> where T : IMemoryReleasable
    {
        private readonly HeapableSlidingWindow<T> _pendingMessages;

        internal MessageSequencer(int size)
        {
            _pendingMessages = new HeapableSlidingWindow<T>(size);
        }

        public T this[int index]
        {
            get
            {
                return _pendingMessages[index];
            }
            set
            {
                _pendingMessages[index] = value;
            }
        }

        public void Release()
        {
            _pendingMessages.Release();
        }
    }
}
