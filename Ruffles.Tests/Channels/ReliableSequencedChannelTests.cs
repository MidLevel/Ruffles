using NUnit.Framework;
using Ruffles.Channeling.Channels;
using Ruffles.Memory;
using Ruffles.Tests.Helpers;
using Ruffles.Tests.Stubs;
using System;

namespace Ruffles.Tests.Channels
{
    [TestFixture()]
    public class ReliableSequencedChannelTests
    {
        [Test()]
        public void TestSimpleMessage()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedChannel clientChannel = new ReliableSequencedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedChannel serverChannel = new ReliableSequencedChannel(0, serversConnectionToClient, config, memoryManager);

            byte[] message = BufferHelper.GetRandomBuffer(1024);

            HeapMemory messageMemory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message, 0, 1024), out bool dealloc)[0];
            ArraySegment<byte>? payload = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset + 2, (int)messageMemory.VirtualCount - 2), out bool hasMore);

            Assert.NotNull(payload);
            Assert.False(hasMore);

            byte[] bytePayload = new byte[payload.Value.Count];
            Array.Copy(payload.Value.Array, payload.Value.Offset, bytePayload, 0, payload.Value.Count);

            Assert.AreEqual(message, bytePayload);
        }

        [Test()]
        public void TestOutOfOrder()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedChannel clientChannel = new ReliableSequencedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedChannel serverChannel = new ReliableSequencedChannel(0, serversConnectionToClient, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024);

            // Sequence all payloads as outgoing
            HeapMemory message1Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024), out bool dealloc)[0];
            HeapMemory message2Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024), out dealloc)[0];
            HeapMemory message3Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024), out dealloc)[0];

            // Consume 1st payload
            ArraySegment<byte>? payload1 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory.Buffer, (int)message1Memory.VirtualOffset + 2, (int)message1Memory.VirtualCount - 2), out bool hasMore1);
            // Consume 3rd payload
            ArraySegment<byte>? payload3 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory.Buffer, (int)message3Memory.VirtualOffset + 2, (int)message3Memory.VirtualCount - 2), out bool hasMore3);
            // Consume 2nd payload
            ArraySegment<byte>? payload2 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory.Buffer, (int)message2Memory.VirtualOffset + 2, (int)message2Memory.VirtualCount - 2), out bool hasMore2);

            HeapMemory pollMemory = serverChannel.HandlePoll();

            {
                Assert.NotNull(payload1);
                Assert.False(hasMore1);

                byte[] bytePayload = new byte[payload1.Value.Count];
                Array.Copy(payload1.Value.Array, payload1.Value.Offset, bytePayload, 0, payload1.Value.Count);

                Assert.AreEqual(message1, bytePayload);
            }

            {
                Assert.Null(payload3);
                Assert.False(hasMore3);
            }

            {
                Assert.NotNull(payload2);
                Assert.True(hasMore2);

                byte[] bytePayload = new byte[payload2.Value.Count];
                Array.Copy(payload2.Value.Array, payload2.Value.Offset, bytePayload, 0, payload2.Value.Count);

                Assert.AreEqual(message2, bytePayload);
            }

            {
                // Check for the third packet
                Assert.NotNull(pollMemory);

                byte[] bytePayload = new byte[pollMemory.VirtualCount];
                Array.Copy(pollMemory.Buffer, pollMemory.VirtualOffset, bytePayload, 0, pollMemory.VirtualCount);

                Assert.AreEqual(message3, bytePayload);
            }
        }
    }
}
