using System;

namespace Ruffles.Messaging
{
    internal static class HeaderPacker
    {
        internal static byte Pack(MessageType messageType)
        {
            if (!Enum.IsDefined(typeof(MessageType), messageType) || messageType == MessageType.Unknown)
            {
                throw new ArgumentException("Has to be a valid MessageType", nameof(messageType));
            }

            // First 4 bits is type
            byte header = (byte)(((byte)messageType) & 15);

            return header;
        }

        internal static void Unpack(byte header, out MessageType messageType)
        {
            // Get first 4 bits
            byte type = (byte)(header & 15);

            if (!Enum.IsDefined(typeof(MessageType), type))
            {
                messageType = MessageType.Unknown;   
            }
            else
            {
                messageType = (MessageType)type;
            }
        }
    }
}
