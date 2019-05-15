namespace Ruffles.Messaging
{
    internal static class HeaderPacker
    {
        internal static byte Pack(byte type, bool fragment)
        {
            byte header = (byte)(type & 15);

            if (fragment)
            {
                header |= 16;
            }

            return header;
        }

        internal static void Unpack(byte header, out byte type, out bool fragment)
        {
            type = (byte)(header & 15);
            fragment = ((header & 16) >> 4) == 1;
        }
    }
}
