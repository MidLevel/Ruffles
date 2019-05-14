using System;

namespace Ruffles.Random
{
    internal static class RandomProvider
    {
        private static readonly System.Random random = new System.Random();
        private static readonly byte[] randomBuffer = new byte[8];

        internal static ulong GetRandomULong()
        {
            random.NextBytes(randomBuffer);

            return (((ulong)randomBuffer[0]) |
                    ((ulong)randomBuffer[1] << 8) |
                    ((ulong)randomBuffer[2] << 16) |
                    ((ulong)randomBuffer[3] << 24) |
                    ((ulong)randomBuffer[4] << 32) |
                    ((ulong)randomBuffer[5] << 40) |
                    ((ulong)randomBuffer[6] << 48) |
                    ((ulong)randomBuffer[7] << 56));
        }
    }
}
