using System;
using Ruffles.Channeling;

namespace Ruffles.Utils
{
    internal static class ChannelTypeUtils
    {
        internal static bool IsValidChannelType(byte channelType)
        {
            return channelType <= 7;
        }

        internal static bool IsValidChannelType(ChannelType channelType)
        {
            return channelType == ChannelType.Reliable ||
                   channelType == ChannelType.Unreliable ||
                   channelType == ChannelType.UnreliableOrdered ||
                   channelType == ChannelType.ReliableSequenced ||
                   channelType == ChannelType.UnreliableRaw ||
                   channelType == ChannelType.ReliableSequencedFragmented ||
                   channelType == ChannelType.ReliableOrdered ||
                   channelType == ChannelType.ReliableFragmented;
        }

        internal static byte ToByte(ChannelType channelType)
        {
            switch (channelType)
            {
                case ChannelType.Reliable:
                    return 0;
                case ChannelType.Unreliable:
                    return 1;
                case ChannelType.UnreliableOrdered:
                    return 2;
                case ChannelType.ReliableSequenced:
                    return 3;
                case ChannelType.UnreliableRaw:
                    return 4;
                case ChannelType.ReliableSequencedFragmented:
                    return 5;
                case ChannelType.ReliableOrdered:
                    return 6;
                case ChannelType.ReliableFragmented:
                    return 7;
                default:
                    throw new ArgumentException("Invalid channel type", nameof(channelType));
            }
        }

        internal static ChannelType FromByte(byte channelType)
        {
            switch (channelType)
            {
                case 0:
                    return ChannelType.Reliable;
                case 1:
                    return ChannelType.Unreliable;
                case 2:
                    return ChannelType.UnreliableOrdered;
                case 3:
                    return ChannelType.ReliableSequenced;
                case 4:
                    return ChannelType.UnreliableRaw;
                case 5:
                    return ChannelType.ReliableSequencedFragmented;
                case 6:
                    return ChannelType.ReliableOrdered;
                case 7:
                    return ChannelType.ReliableFragmented;
                default:
                    throw new ArgumentException("Invalid channel type", nameof(channelType));
            }
        }
    }
}
