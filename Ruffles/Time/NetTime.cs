using System;
using System.Diagnostics;

namespace Ruffles.Time
{
    public struct NetTime
    {
        private static readonly bool HighResolution = Stopwatch.IsHighResolution;
        private static readonly long StartTime = Stopwatch.GetTimestamp();
        private static readonly DateTime StartDate = DateTime.Now;
        private static readonly double MillisecondsPerTick = 1000d / Stopwatch.Frequency;

        internal static NetTime Now => new NetTime(Stopwatch.GetTimestamp() - StartTime);
        internal static NetTime MinValue => new NetTime(StartTime);

        private readonly long InternalTicks;

        public long Milliseconds => (long)(InternalTicks * MillisecondsPerTick);
        public DateTime Date => StartDate.AddMilliseconds(Milliseconds);

        private NetTime(long ticks)
        {
            InternalTicks = ticks;
        }

        public NetTime AddMilliseconds(double milliseconds)
        {
            return new NetTime(InternalTicks + (long)(milliseconds * MillisecondsPerTick));
        }

        public static TimeSpan operator -(NetTime t1, NetTime t2) => new TimeSpan(0, 0, 0, 0, (int)((t1.InternalTicks - t2.InternalTicks) * MillisecondsPerTick));
        public static bool operator ==(NetTime t1, NetTime t2) => t1.InternalTicks == t2.InternalTicks;
        public static bool operator !=(NetTime t1, NetTime t2) => t1.InternalTicks != t2.InternalTicks;
        public static bool operator <(NetTime t1, NetTime t2) => t1.InternalTicks < t2.InternalTicks;
        public static bool operator <=(NetTime t1, NetTime t2) => t1.InternalTicks <= t2.InternalTicks;
        public static bool operator >(NetTime t1, NetTime t2) => t1.InternalTicks > t2.InternalTicks;
        public static bool operator >=(NetTime t1, NetTime t2) => t1.InternalTicks >= t2.InternalTicks;

        public override int GetHashCode()
        {
            long ticks = InternalTicks;
            return unchecked((int)ticks) ^ (int)(ticks >> 32);
        }

        public override bool Equals(object obj)
        {
            if (obj is NetTime)
            {
                return InternalTicks == ((NetTime)obj).InternalTicks;
            }

            return false;
        }
    }
}
