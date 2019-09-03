namespace Ruffles.Tests.Helpers
{
    public static class BufferHelper
    {
        public static byte[] GetRandomBuffer(int size)
        {
            System.Random rnd = new System.Random();

            byte[] buffer = new byte[size];

            rnd.NextBytes(buffer);

            return buffer;
        }
    }
}
