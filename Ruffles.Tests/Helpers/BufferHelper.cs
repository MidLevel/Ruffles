using System;
using Ruffles.Memory;

namespace Ruffles.Tests.Helpers
{
    public static class BufferHelper
    {
        internal static byte[] GetRandomBuffer(int size, byte identity)
        {
            byte[] buffer = new byte[size];

            byte identityCounter = identity;

            for (int i = 0; i < buffer.Length; i++)
            {
                buffer[i] = identityCounter;

                identityCounter++;
            }

            return buffer;
        }

        internal static bool ValidateIdentity(byte[] memory, byte identity)
        {
            return ValidateIdentity(new ArraySegment<byte>(memory, 0, memory.Length), identity);
        }

        internal static bool ValidateIdentity(HeapMemory memory, byte identity)
        {
            return ValidateIdentity(new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount), identity);
        }

        internal static bool ValidateIdentity(ArraySegment<byte> buffer, byte identity)
        {
            byte identityCounter = identity;

            for (int i = 0; i < buffer.Count; i++)
            {
                if (buffer.Array[buffer.Offset + i] != identityCounter)
                {
                    return false;
                }

                identityCounter++;
            }

            return true;
        }

        internal static bool ValidateBufferSize(HeapMemory memory, int size)
        {
            return ValidateBufferSize(new ArraySegment<byte>(memory.Buffer, (int)memory.VirtualOffset, (int)memory.VirtualCount), size);
        }

        internal static bool ValidateBufferSize(byte[] buffer, int size)
        {
            return buffer.Length == size;
        }

        internal static bool ValidateBufferSize(ArraySegment<byte> buffer, int size)
        {
            return buffer.Count == size;
        }

        internal static bool ValidateBufferSizeAndIdentity(byte[] buffer, byte identity, int size)
        {
            return ValidateIdentity(buffer, identity) && ValidateBufferSize(buffer, size);
        }

        internal static bool ValidateBufferSizeAndIdentity(ArraySegment<byte> buffer, byte identity, int size)
        {
            return ValidateIdentity(buffer, identity) && ValidateBufferSize(buffer, size);
        }

        internal static bool ValidateBufferSizeAndIdentity(HeapMemory buffer, byte identity, int size)
        {
            return ValidateIdentity(buffer, identity) && ValidateBufferSize(buffer, size);
        }
    }
}
