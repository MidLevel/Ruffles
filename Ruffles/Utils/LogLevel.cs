namespace Ruffles.Utils
{
    /// <summary>
    /// Log level
    /// </summary>
    public enum LogLevel
    {
        /// <summary>
        /// Detailed steps of every event.
        /// </summary>
        Debug,
        /// <summary>
        /// General events such as when a client connects.
        /// </summary>
        Info,
        /// <summary>
        /// A potential problem has occured. It doesnt prevent us from continuing. This occurs for things that might be others fault, such as invalid configurations.
        /// </summary>
        Warning,
        /// <summary>
        /// An error that affects us occured. Usually means the fault of us.
        /// </summary>
        Error,
        /// <summary>
        /// Logs nothing.
        /// </summary>
        Nothing
    }
}