namespace Ruffles.Connections
{
    /// <summary>
    /// Enum representing the connection state between two RuffleSockets.
    /// </summary>
    public enum ConnectionState : byte
    {
        /// <summary>
        /// The local peer has requested a connection.
        /// </summary>
        RequestingConnection,
        /// <summary>
        /// The local peer has requested a challenge to be solved.
        /// </summary>
        RequestingChallenge,
        /// <summary>
        /// The local peer is solving the challenge.
        /// </summary>
        SolvingChallenge,
        /// <summary>
        /// The connection is established.
        /// </summary>
        Connected
    }
}
