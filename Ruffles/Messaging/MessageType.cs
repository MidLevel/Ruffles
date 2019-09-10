namespace Ruffles.Messaging
{
    internal enum MessageType : byte
    {
        ConnectionRequest,
        ChallengeRequest,
        ChallengeResponse,
        Hail,
        HailConfirmed,
        Heartbeat,
        Data,
        Disconnect,
        Ack,
        Merge,
        UnconnectedData,
        MTURequest,
        MTUResponse
    }
}
