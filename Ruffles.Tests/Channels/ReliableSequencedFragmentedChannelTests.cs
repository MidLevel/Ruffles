using NUnit.Framework;
using Ruffles.Channeling.Channels;
using Ruffles.Memory;
using Ruffles.Tests.Helpers;
using Ruffles.Tests.Stubs;
using System;

namespace Ruffles.Tests.Channels
{
    [TestFixture()]
    public class ReliableSequencedFragmentedChannelTests
    {
        [Test()]
        public void TestSimpleMessageSingleFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, null, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, null, config, memoryManager);

            byte[] message = BufferHelper.GetRandomBuffer(1024);

            HeapMemory messageMemory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message, 0, 1024), out bool dealloc)[0];
            ArraySegment<byte>? payload = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(messageMemory.Buffer, (int)messageMemory.VirtualOffset + 2, (int)messageMemory.VirtualCount - 2), out bool hasMore);

            Assert.Null(payload);
            Assert.True(hasMore);

            HeapMemory payloadMemory = serverChannel.HandlePoll();

            Assert.NotNull(payloadMemory);

            byte[] bytePayload = new byte[payloadMemory.VirtualCount];
            Array.Copy(payloadMemory.Buffer, payloadMemory.VirtualOffset, bytePayload, 0, payloadMemory.VirtualCount);

            Assert.AreEqual(message, bytePayload);
        }

        [Test()]
        public void TestOutOfOrderSingleFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, null, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, null, config, memoryManager);

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

            Assert.Null(payload1);
            Assert.Null(payload2);
            Assert.Null(payload3);

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
                byte[] bytePayload = new byte[poll1.VirtualCount];
                Array.Copy(poll1.Buffer, poll1.VirtualOffset, bytePayload, 0, poll1.VirtualCount);

                Assert.AreEqual(message1, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll2.VirtualCount];
                Array.Copy(poll2.Buffer, poll2.VirtualOffset, bytePayload, 0, poll2.VirtualCount);

                Assert.AreEqual(message2, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll3.VirtualCount];
                Array.Copy(poll3.Buffer, poll3.VirtualOffset, bytePayload, 0, poll3.VirtualCount);

                Assert.AreEqual(message3, bytePayload);
            }
        }

        [Test()]
        public void TestOutOfFragmentOrderMultiFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, null, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, null, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024 * 5);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024 * 8);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024 * 6);

            // Sequence all payloads as outgoing
            HeapMemory[] message1Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024 * 5), out bool dealloc);
            HeapMemory[] message2Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024 * 8), out dealloc);
            HeapMemory[] message3Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024 * 6), out dealloc);

            // Consume 1st payload all except first fragment
            bool[] hasMore1s = new bool[message1Memory.Length];
            ArraySegment<byte>?[] payload1s = new ArraySegment<byte>?[message1Memory.Length];
            for (int i = 1; i < message1Memory.Length; i++)
            {
                payload1s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory[i].Buffer, (int)message1Memory[i].VirtualOffset + 2, (int)message1Memory[i].VirtualCount - 2), out bool hasMore1);
                hasMore1s[i] = hasMore1;
            }
            // Consume 3rd payload only last fragment
            bool[] hasMore3s = new bool[message3Memory.Length];
            ArraySegment<byte>?[] payload3s = new ArraySegment<byte>?[message3Memory.Length];

            {
                payload3s[payload3s.Length - 1] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory[payload3s.Length - 1].Buffer, (int)message3Memory[payload3s.Length - 1].VirtualOffset + 2, (int)message3Memory[payload3s.Length - 1].VirtualCount - 2), out bool hasMore3);
                hasMore3s[payload3s.Length - 1] = hasMore3;
            }

            // Consume 2nd payload all fragments
            bool[] hasMore2s = new bool[message2Memory.Length];
            ArraySegment<byte>?[] payload2s = new ArraySegment<byte>?[message2Memory.Length];
            for (int i = 0; i < message2Memory.Length; i++)
            {
                payload2s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory[i].Buffer, (int)message2Memory[i].VirtualOffset + 2, (int)message2Memory[i].VirtualCount - 2), out bool hasMore2);
                hasMore2s[i] = hasMore2;
            }

            {
                // Consume 3rd payload all except last fragment (completes the last payload)
                for (int i = 0; i < message3Memory.Length - 1; i++)
                {
                    payload3s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory[i].Buffer, (int)message3Memory[i].VirtualOffset + 2, (int)message3Memory[i].VirtualCount - 2), out bool hasMore3);
                    hasMore3s[i] = hasMore3;
                }
            }

            {
                // Consume 1st payload first fragment (completes first payload)
                payload1s[0] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory[0].Buffer, (int)message1Memory[0].VirtualOffset + 2, (int)message1Memory[0].VirtualCount - 2), out bool hasMore1);
                hasMore1s[0] = hasMore1;
            }

            for (int i = 0; i < payload1s.Length; i++)
            {
                Assert.Null(payload1s[i]);
            }

            for (int i = 0; i < payload3s.Length; i++)
            {
                Assert.Null(payload3s[i]);
            }

            for (int i = 0; i < payload2s.Length; i++)
            {
                Assert.Null(payload2s[i]);
            }

            // TODO: Assert HasMore states

            HeapMemory poll1 = serverChannel.HandlePoll();
            HeapMemory poll2 = serverChannel.HandlePoll();
            HeapMemory poll3 = serverChannel.HandlePoll();

            Assert.NotNull(poll1, "p1");
            Assert.NotNull(poll2, "p2");
            Assert.NotNull(poll3, "p3");

            {
                byte[] bytePayload = new byte[poll1.VirtualCount];
                Array.Copy(poll1.Buffer, poll1.VirtualOffset, bytePayload, 0, poll1.VirtualCount);

                Assert.AreEqual(message1, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll2.VirtualCount];
                Array.Copy(poll2.Buffer, poll2.VirtualOffset, bytePayload, 0, poll2.VirtualCount);

                Assert.AreEqual(message2, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll3.VirtualCount];
                Array.Copy(poll3.Buffer, poll3.VirtualOffset, bytePayload, 0, poll3.VirtualCount);

                Assert.AreEqual(message3, bytePayload);
            }
        }

        [Test()]
        public void TestOutOfPacketOrderMultiFragment()
        {
            Configuration.SocketConfig config = new Configuration.SocketConfig();

            MemoryManager memoryManager = new MemoryManager(config);

            ConnectionStub clientsConnectionToServer = new ConnectionStub();
            ConnectionStub serversConnectionToClient = new ConnectionStub();

            ReliableSequencedFragmentedChannel clientChannel = new ReliableSequencedFragmentedChannel(0, clientsConnectionToServer, null, config, memoryManager);
            ReliableSequencedFragmentedChannel serverChannel = new ReliableSequencedFragmentedChannel(0, serversConnectionToClient, null, config, memoryManager);

            // Create 3 payloads
            byte[] message1 = BufferHelper.GetRandomBuffer(1024 * 5);
            byte[] message2 = BufferHelper.GetRandomBuffer(1024 * 8);
            byte[] message3 = BufferHelper.GetRandomBuffer(1024 * 6);

            // Sequence all payloads as outgoing
            HeapMemory[] message1Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message1, 0, 1024 * 5), out bool dealloc);
            HeapMemory[] message2Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message2, 0, 1024 * 8), out dealloc);
            HeapMemory[] message3Memory = clientChannel.CreateOutgoingMessage(new ArraySegment<byte>(message3, 0, 1024 * 6), out dealloc);

            // Consume 1st payload
            bool[] hasMore1s = new bool[message1Memory.Length];
            ArraySegment<byte>?[] payload1s = new ArraySegment<byte>?[message1Memory.Length];
            for (int i = 0; i < message1Memory.Length; i++)
            {
                payload1s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message1Memory[i].Buffer, (int)message1Memory[i].VirtualOffset + 2, (int)message1Memory[i].VirtualCount - 2), out bool hasMore1);
                hasMore1s[i] = hasMore1;
            }
            // Consume 3rd payload
            bool[] hasMore3s = new bool[message3Memory.Length];
            ArraySegment<byte>?[] payload3s = new ArraySegment<byte>?[message3Memory.Length];
            for (int i = 0; i < message3Memory.Length; i++)
            {
                payload3s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message3Memory[i].Buffer, (int)message3Memory[i].VirtualOffset + 2, (int)message3Memory[i].VirtualCount - 2), out bool hasMore3);
                hasMore3s[i] = hasMore3;
            }
            // Consume 2nd payload
            bool[] hasMore2s = new bool[message2Memory.Length];
            ArraySegment<byte>?[] payload2s = new ArraySegment<byte>?[message2Memory.Length];
            for (int i = 0; i < message2Memory.Length; i++)
            {
                payload2s[i] = serverChannel.HandleIncomingMessagePoll(new ArraySegment<byte>(message2Memory[i].Buffer, (int)message2Memory[i].VirtualOffset + 2, (int)message2Memory[i].VirtualCount - 2), out bool hasMore2);
                hasMore2s[i] = hasMore2;
            }


            for (int i = 0; i < payload1s.Length; i++)
            {
                Assert.Null(payload1s[i]);
            }

            for (int i = 0; i < payload3s.Length; i++)
            {
                Assert.Null(payload3s[i]);
            }

            for (int i = 0; i < payload2s.Length; i++)
            {
                Assert.Null(payload2s[i]);
            }

            // TODO: Assert HasMore states

            HeapMemory poll1 = serverChannel.HandlePoll();
            HeapMemory poll2 = serverChannel.HandlePoll();
            HeapMemory poll3 = serverChannel.HandlePoll();

            Assert.NotNull(poll1);
            Assert.NotNull(poll2);
            Assert.NotNull(poll3);

            {
                byte[] bytePayload = new byte[poll1.VirtualCount];
                Array.Copy(poll1.Buffer, poll1.VirtualOffset, bytePayload, 0, poll1.VirtualCount);

                Assert.AreEqual(message1, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll2.VirtualCount];
                Array.Copy(poll2.Buffer, poll2.VirtualOffset, bytePayload, 0, poll2.VirtualCount);

                Assert.AreEqual(message2, bytePayload);
            }

            {
                byte[] bytePayload = new byte[poll3.VirtualCount];
                Array.Copy(poll3.Buffer, poll3.VirtualOffset, bytePayload, 0, poll3.VirtualCount);

                Assert.AreEqual(message3, bytePayload);
            }
        }
    }
}
