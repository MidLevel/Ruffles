using NUnit.Framework;
using Ruffles.Channeling.Channels;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Tests.Helpers;
using System;

namespace Ruffles.Tests.Channels
{
    [TestFixture()]
    public class ReliableSequencedFragmentedChannelTests
    {
        [Test()]
        public void TestSimpleMessageSingleFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig()
            {
                MinimumMTU = 1050
            };

            MemoryManager memoryManager = new MemoryManager(config);

            Connection clientsConnectionToServer = Connection.Stub(config);
            Connection serversConnectionToClient = Connection.Stub(config);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            byte[] message = BufferHelper.GetRandomBuffer(1024, 0);

            HeapMemory messageMemory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message, 0, 1024), out bool dealloc).Pointers[0]);
            HeapPointers payload = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset + 2, (int)messageMemory.VirtualCount - 2));

            Assert.NotNull(payload);

            Assert.NotNull(payload.Pointers);

            Assert.AreEqual(1, payload.VirtualCount);

            MemoryWrapper wrapper = (MemoryWrapper)payload.Pointers[0];

            Assert.NotNull(wrapper);

            Assert.NotNull(wrapper.AllocatedMemory);
            Assert.Null(wrapper.DirectMemory);

            Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(wrapper.AllocatedMemory, 0, 1024));
        }

        [Test()]
        public void TestOutOfOrderSingleFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig()
            {
                MinimumMTU = 1050
            };

            MemoryManager memoryManager = new MemoryManager(config);

            Connection clientsConnectionToServer = Connection.Stub(config);
            Connection serversConnectionToClient = Connection.Stub(config);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024, 0);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024, 1);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024, 2);

            // Sequence all payloads as outgoing
            HeapMemory message1Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024), out bool dealloc).Pointers[0]);
            HeapMemory message2Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024), out dealloc).Pointers[0]);
            HeapMemory message3Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024), out dealloc).Pointers[0]);

            // Consume 1st payload
            HeapPointers payload1 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory.Buffer, (int)message1Memory.VirtualOffset + 2, (int)message1Memory.VirtualCount - 2));
            // Consume 3rd payload
            HeapPointers payload3 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory.Buffer, (int)message3Memory.VirtualOffset + 2, (int)message3Memory.VirtualCount - 2));
            // Consume 2nd payload
            HeapPointers payload2 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory.Buffer, (int)message2Memory.VirtualOffset + 2, (int)message2Memory.VirtualCount - 2));

            {
                Assert.NotNull(payload1);
                Assert.NotNull(payload1.Pointers);
                Assert.AreEqual(1, payload1.VirtualCount);
                Assert.NotNull(payload1.Pointers[0]);
                MemoryWrapper wrapper = (MemoryWrapper)payload1.Pointers[0];
                Assert.NotNull(wrapper);
                Assert.Null(wrapper.DirectMemory);
                Assert.NotNull(wrapper.AllocatedMemory);
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(wrapper.AllocatedMemory, 0, 1024));
            }

            {
                Assert.Null(payload3);
            }

            {
                Assert.NotNull(payload2);
                Assert.NotNull(payload2.Pointers);
                Assert.AreEqual(2, payload2.VirtualCount);

                Assert.NotNull(payload2.Pointers[0]);
                Assert.NotNull(payload2.Pointers[1]);

                MemoryWrapper wrapper1 = (MemoryWrapper)payload2.Pointers[0];
                MemoryWrapper wrapper2 = (MemoryWrapper)payload2.Pointers[1];

                Assert.NotNull(wrapper1);
                Assert.NotNull(wrapper2);

                Assert.NotNull(wrapper1.AllocatedMemory);
                Assert.NotNull(wrapper2.AllocatedMemory);
                Assert.Null(wrapper1.DirectMemory);
                Assert.Null(wrapper2.DirectMemory);

                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(wrapper1.AllocatedMemory, 1, 1024));
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(wrapper2.AllocatedMemory, 2, 1024));
            }
        }
    }
}
