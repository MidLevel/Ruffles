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

            Connection clientsConnectionToServer = Connection.Stub(config, memoryManager);
            Connection serversConnectionToClient = Connection.Stub(config, memoryManager);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            byte[] message = BufferHelper.GetRandomBuffer(1024, 0);

            HeapMemory messageMemory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message, 0, 1024), out _, out bool dealloc).Pointers[0]);
            HeapPointers payload = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset + 2, (int)messageMemory.VirtualCount - 2), out _);

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

            Connection clientsConnectionToServer = Connection.Stub(config, memoryManager);
            Connection serversConnectionToClient = Connection.Stub(config, memoryManager);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024, 0);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024, 1);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024, 2);

            // Sequence all payloads as outgoing
            HeapMemory message1Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024), out _, out bool dealloc).Pointers[0]);
            HeapMemory message2Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024), out _, out dealloc).Pointers[0]);
            HeapMemory message3Memory = ((HeapMemory)clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024), out _, out dealloc).Pointers[0]);

            // Consume 1st payload
            HeapPointers payload1 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory.Buffer, (int)message1Memory.VirtualOffset + 2, (int)message1Memory.VirtualCount - 2), out _);
            // Consume 3rd payload
            HeapPointers payload3 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory.Buffer, (int)message3Memory.VirtualOffset + 2, (int)message3Memory.VirtualCount - 2), out _);
            // Consume 2nd payload
            HeapPointers payload2 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory.Buffer, (int)message2Memory.VirtualOffset + 2, (int)message2Memory.VirtualCount - 2), out _);

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

        /*
        [Test()]
        public void TestOutOfFragmentOrderMultiFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig()
            {
                MinimumMTU = 1050
            };

            MemoryManager memoryManager = new MemoryManager(config);

            Connection clientsConnectionToServer = Connection.Stub(config, memoryManager);
            Connection serversConnectionToClient = Connection.Stub(config, memoryManager);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024 * 5, 0);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024 * 8, 1);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024 * 6, 2);

            // Sequence all payloads as outgoing
            HeapPointers message1Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024 * 5), out _, out bool dealloc);
            HeapPointers message2Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024 * 8), out _, out dealloc);
            HeapPointers message3Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024 * 6), out _, out dealloc);

            int message1MemoryLength = (int)message1Memory.VirtualCount;
            int message2MemoryLength = (int)message2Memory.VirtualCount;
            int message3MemoryLength = (int)message3Memory.VirtualCount;

            // Consume 1st payload all except first fragment
            bool[] hasMore1s = new bool[message1MemoryLength];
            DirectOrAllocedMemory[] payload1s = new DirectOrAllocedMemory[message1MemoryLength];
            for (int i = 1; i < message1MemoryLength; i++)
            {
                payload1s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message1Memory.Pointers[i]).Buffer, (int)((HeapMemory)message1Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message1Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore1);
                hasMore1s[i] = hasMore1;
            }
            // Consume 3rd payload only last fragment
            bool[] hasMore3s = new bool[message3MemoryLength];
            DirectOrAllocedMemory[] payload3s = new DirectOrAllocedMemory[message3MemoryLength];

            {
                payload3s[payload3s.Length - 1] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message3Memory.Pointers[payload3s.Length - 1]).Buffer, (int)((HeapMemory)message3Memory.Pointers[payload3s.Length - 1]).VirtualOffset + 2, (int)((HeapMemory)message3Memory.Pointers[payload3s.Length - 1]).VirtualCount - 2), out _, out bool hasMore3);
                hasMore3s[payload3s.Length - 1] = hasMore3;
            }

            // Consume 2nd payload all fragments
            bool[] hasMore2s = new bool[message2MemoryLength];
            DirectOrAllocedMemory[] payload2s = new DirectOrAllocedMemory[message2MemoryLength];
            for (int i = 0; i < message2MemoryLength; i++)
            {
                payload2s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message2Memory.Pointers[i]).Buffer, (int)((HeapMemory)message2Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message2Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore2);
                hasMore2s[i] = hasMore2;
            }

            {
                // Consume 3rd payload all except last fragment (completes the last payload)
                for (int i = 0; i < message3MemoryLength - 1; i++)
                {
                    payload3s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message3Memory.Pointers[i]).Buffer, (int)((HeapMemory)message3Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message3Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore3);
                    hasMore3s[i] = hasMore3;
                }
            }

            {
                // Consume 1st payload first fragment (completes first payload)
                payload1s[0] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message1Memory.Pointers[0]).Buffer, (int)((HeapMemory)message1Memory.Pointers[0]).VirtualOffset + 2, (int)((HeapMemory)message1Memory.Pointers[0]).VirtualCount - 2), out _, out bool hasMore1);
                hasMore1s[0] = hasMore1;
            }

            for (int i = 0; i < payload1s.Length; i++)
            {
                Assert.Null(payload1s[i].AllocedMemory);
                Assert.Null(payload1s[i].DirectMemory);
            }

            for (int i = 0; i < payload3s.Length; i++)
            {
                Assert.Null(payload3s[i].AllocedMemory);
                Assert.Null(payload1s[i].DirectMemory);
            }

            for (int i = 0; i < payload2s.Length; i++)
            {
                Assert.Null(payload2s[i].AllocedMemory);
                Assert.Null(payload2s[i].DirectMemory);
            }

            // TODO: Assert HasMore states

            HeapMemory poll1 = serverChannel.HandlePoll();
            HeapMemory poll2 = serverChannel.HandlePoll();
            HeapMemory poll3 = serverChannel.HandlePoll();

            Assert.NotNull(poll1, "p1");
            Assert.NotNull(poll2, "p2");
            Assert.NotNull(poll3, "p3");

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll1, 0, 1024 * 5));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll2, 1, 1024 * 8));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll3, 2, 1024 * 6));
            }
        }

        [Test()]
        public void TestOutOfPacketOrderMultiFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig()
            {
                MinimumMTU = 1050
            };

            MemoryManager memoryManager = new MemoryManager(config);

            Connection clientsConnectionToServer = Connection.Stub(config, memoryManager);
            Connection serversConnectionToClient = Connection.Stub(config, memoryManager);

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024 * 5, 0);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024 * 8, 1);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024 * 6, 2);

            // Sequence all payloads as outgoing
            HeapPointers message1Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024 * 5), out _, out bool dealloc);
            HeapPointers message2Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024 * 8), out _, out dealloc);
            HeapPointers message3Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024 * 6), out _, out dealloc);

            int message1MemoryLength = (int)message1Memory.VirtualCount;
            int message2MemoryLength = (int)message2Memory.VirtualCount;
            int message3MemoryLength = (int)message3Memory.VirtualCount;

            // Consume 1st payload
            bool[] hasMore1s = new bool[message1MemoryLength];
            DirectOrAllocedMemory[] payload1s = new DirectOrAllocedMemory[message1MemoryLength];
            for (int i = 0; i < message1MemoryLength; i++)
            {
                payload1s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message1Memory.Pointers[i]).Buffer, (int)((HeapMemory)message1Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message1Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore1);
                hasMore1s[i] = hasMore1;
            }
            // Consume 3rd payload
            bool[] hasMore3s = new bool[message3MemoryLength];
            DirectOrAllocedMemory[] payload3s = new DirectOrAllocedMemory[message3MemoryLength];
            for (int i = 0; i < message3MemoryLength; i++)
            {
                payload3s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message3Memory.Pointers[i]).Buffer, (int)((HeapMemory)message3Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message3Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore3);
                hasMore3s[i] = hasMore3;
            }
            // Consume 2nd payload
            bool[] hasMore2s = new bool[message2MemoryLength];
            DirectOrAllocedMemory[] payload2s = new DirectOrAllocedMemory[message2MemoryLength];
            for (int i = 0; i < message2MemoryLength; i++)
            {
                payload2s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(((HeapMemory)message2Memory.Pointers[i]).Buffer, (int)((HeapMemory)message2Memory.Pointers[i]).VirtualOffset + 2, (int)((HeapMemory)message2Memory.Pointers[i]).VirtualCount - 2), out _, out bool hasMore2);
                hasMore2s[i] = hasMore2;
            }

            for (int i = 0; i < payload1s.Length; i++)
            {
                Assert.Null(payload1s[i].AllocedMemory);
                Assert.Null(payload1s[i].DirectMemory);
            }

            for (int i = 0; i < payload3s.Length; i++)
            {
                Assert.Null(payload3s[i].AllocedMemory);
                Assert.Null(payload1s[i].DirectMemory);
            }

            for (int i = 0; i < payload2s.Length; i++)
            {
                Assert.Null(payload2s[i].AllocedMemory);
                Assert.Null(payload2s[i].DirectMemory);
            }

            // TODO: Assert HasMore states

            HeapMemory poll1 = serverChannel.HandlePoll();
            HeapMemory poll2 = serverChannel.HandlePoll();
            HeapMemory poll3 = serverChannel.HandlePoll();

            Assert.NotNull(poll1);
            Assert.NotNull(poll2);
            Assert.NotNull(poll3);

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll1, 0, 1024 * 5));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll2, 1, 1024 * 8));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll3, 2, 1024 * 6));
            }
        }
        */
    }
}
