using NUnit.Framework;
using Ruffles.Channeling.Channels;
using Ruffles.Connections;
using Ruffles.Memory;
using Ruffles.Tests.Helpers;
using System;
using System.Linq;

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
            DirectOrAllocedMemory payload = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset + 2, (int)messageMemory.VirtualCount - 2), out _, out bool hasMore);

            Assert.Null(payload.AllocedMemory);
            Assert.Null(payload.DirectMemory);
            Assert.True(hasMore);

            HeapMemory payloadMemory = serverChannel.HandlePoll();

            Assert.NotNull(payloadMemory);

            Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(payloadMemory, 0, 1024));
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
            DirectOrAllocedMemory payload1 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory.Buffer, (int)message1Memory.VirtualOffset + 2, (int)message1Memory.VirtualCount - 2), out _, out bool hasMore1);
            // Consume 3rd payload
            DirectOrAllocedMemory payload3 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory.Buffer, (int)message3Memory.VirtualOffset + 2, (int)message3Memory.VirtualCount - 2), out _, out bool hasMore3);
            // Consume 2nd payload
            DirectOrAllocedMemory payload2 = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory.Buffer, (int)message2Memory.VirtualOffset + 2, (int)message2Memory.VirtualCount - 2), out _, out bool hasMore2);

            Assert.Null(payload1.AllocedMemory);
            Assert.Null(payload2.AllocedMemory);
            Assert.Null(payload3.AllocedMemory);

            Assert.Null(payload1.DirectMemory);
            Assert.Null(payload2.DirectMemory);
            Assert.Null(payload3.DirectMemory);

            Assert.True(hasMore1);
            Assert.True(hasMore2);
            Assert.True(hasMore3);

            HeapMemory poll1 = serverChannel.HandlePoll();
            HeapMemory poll2 = serverChannel.HandlePoll();
            HeapMemory poll3 = serverChannel.HandlePoll();

            Assert.NotNull(poll1);
            Assert.NotNull(poll2);
            Assert.NotNull(poll3);

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll1, 0, 1024));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll2, 1, 1024));
            }

            {
                Assert.That(BufferHelper.ValidateBufferSizeAndIdentity(poll3, 2, 1024));
            }
        }

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
    }
}
