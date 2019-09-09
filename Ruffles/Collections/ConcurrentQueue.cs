using System.Collections.Generic;

namespace Ruffles.Collections
{
    internal class ConcurrentQueue<T>
    {
        private readonly Queue<T> _queue = new Queue<T>();
        private readonly object _lock = new object();

        public bool TryDequeue(out T element)
        {
            lock (_lock)
            {
                if (_queue.Count > 0)
                {
                    element = _queue.Dequeue();

                    return true;
                }
            }

            element = default(T);

            return false;
        }

        public void Enqueue(T element)
        {
            lock (_lock)
            {
                _queue.Enqueue(element);
            }
        }
    }
}
