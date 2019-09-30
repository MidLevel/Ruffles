using System;
using System.Collections.Generic;

namespace Ruffles.Messaging
{
    internal class MessageMerger
    {
        private readonly byte[] _buffer;
        private int _position;
        private DateTime _lastFlushTime;
        private ulong _flushDelay;
        private ushort _headerBytes;
        private ushort _packets;

        private readonly object _lock = new object();

        internal MessageMerger(int maxSize, ulong flushDelay)
        {
            _buffer = new byte[maxSize];
            _buffer[0] = HeaderPacker.Pack((byte)MessageType.Merge, false);
            _position = 1;
            _headerBytes = 1;
            _packets = 0;
            _lastFlushTime = DateTime.Now;
            _flushDelay = flushDelay;
        }

        internal void Clear()
        {
            lock (_lock)
            {
                _lastFlushTime = DateTime.Now;
                _position = 1;
                _headerBytes = 1;
                _packets = 0;
            }
        }

        internal bool TryWrite(ArraySegment<byte> payload, ushort headerBytes)
        {
            lock (_lock)
            {
                if (payload.Count + _position + 2 > _buffer.Length)
                {
                    // Wont fit
                    return false;
                }
                else
                {
                    // TODO: VarInt
                    // Write the segment size
                    _buffer[_position] = (byte)(payload.Count);
                    _buffer[_position + 1] = (byte)(payload.Count >> 8);

                    // Copy the payload with the header
                    Buffer.BlockCopy(payload.Array, payload.Offset, _buffer, _position + 2, payload.Count);

                    // Update the position
                    _position += 2 + payload.Count;

                    // Update the amount of header bytes
                    _headerBytes += headerBytes;

                    // Update the amount of packets
                    _packets++;

                    return true;
                }
            }
        }

        internal ArraySegment<byte>? TryFlush(out ushort headerBytes)
        {
            lock (_lock)
            {
                if (_position > 1 && (DateTime.Now - _lastFlushTime).TotalMilliseconds > _flushDelay)
                {
                    // Its time to flush

                    // Save the size
                    int flushSize = _position;

                    headerBytes = _headerBytes;

                    // Reset values
                    _position = 1;
                    _lastFlushTime = DateTime.Now;
                    _headerBytes = 1;
                    _packets = 0;

                    return new ArraySegment<byte>(_buffer, 0, flushSize);
                }

                headerBytes = 0;

                return null;
            }
        }


        // DONT MAKE STATIC FOR THREAD SAFETY.
        private readonly List<ArraySegment<byte>> _unpackSegments = new List<ArraySegment<byte>>();

        // DONT MAKE STATIC FOR THREAD SAFETY.
        internal List<ArraySegment<byte>> Unpack(ArraySegment<byte> payload)
        {
            lock (_lock)
            {
                // TODO: VarInt
                if (payload.Count < 3)
                {
                    // Payload is too small
                    return null;
                }

                // Clear the segments list
                _unpackSegments.Clear();

                // The offset for walking the buffer
                int packetOffset = 0;

                while (true)
                {
                    if (payload.Count < packetOffset + 2)
                    {
                        // No more data to be read
                        return _unpackSegments;
                    }

                    // TODO: VarInt
                    // Read the size
                    ushort size = (ushort)(payload.Array[payload.Offset + packetOffset] | (ushort)(payload.Array[payload.Offset + packetOffset + 1] << 8));

                    if (size < 1)
                    {
                        // The size is too small. Doesnt fit the header
                        return _unpackSegments;
                    }

                    // Make sure the size can even fit
                    if (payload.Count < (packetOffset + 2 + size))
                    {
                        // Payload is too small to fit the claimed size. Exit
                        return _unpackSegments;
                    }

                    // Read the header
                    HeaderPacker.Unpack(payload.Array[payload.Offset + packetOffset + 2], out byte type, out bool fragment);

                    // Prevent merging a merge
                    if (type != (byte)MessageType.Merge)
                    {
                        // Add the new segment
                        _unpackSegments.Add(new ArraySegment<byte>(payload.Array, payload.Offset + packetOffset + 2, size));
                    }

                    // Increment the packetOffset
                    packetOffset += 2 + size;
                }
            }
        }
    }
}
