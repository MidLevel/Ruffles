namespace Ruffles.Connections
{
    internal enum ConnectionState : byte
    {
        RequestingConnection,
        RequestingChallenge,
        SolvingChallenge,
        Connected
    }
}
