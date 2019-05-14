using System;

namespace Ruffles.Messaging
{
    internal struct MessageStatus
    {
        public bool HasAcked;
        public byte Attempts;
        public DateTime LastAttempt;

        public bool Completed => HasAcked;
    }
}
