namespace Ruffles.Connections
{
    public enum ConnectionState : byte
    {
        RequestingConnection,
        RequestingChallenge,
        SolvingChallenge,
        Connected
    }
}
