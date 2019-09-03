using System;

namespace Ruffles.Utils
{
    /// <summary>
    /// Logging utils.
    /// </summary>
    public static class Logging
    {
        /// <summary>
        /// Occurs when ruffles spits out an info log.
        /// </summary>
        public static event Action<string> OnInfoLog = (value) => Console.WriteLine("[INFO] " + value);
        /// <summary>
        /// Occurs when ruffles spits out a warning log.
        /// </summary>
        public static event Action<string> OnWarningLog = (value) => Console.WriteLine("[WARNING] " + value);
        /// <summary>
        /// Occurs when ruffles spits out an error log.
        /// </summary>
        public static event Action<string> OnErrorLog = (value) => Console.WriteLine("[ERROR] " + value);

        internal static void Info(string value)
        {
            if (OnInfoLog != null)
            {
                OnInfoLog(value);
            }
        }

        internal static void Warning(string value)
        {
            if (OnWarningLog != null)
            {
                OnWarningLog(value);
            }
        }

        internal static void Error(string value)
        {
            if (OnErrorLog != null)
            {
                OnErrorLog(value);
            }
        }
    }
}
