using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Request;
using Kafka.Client.Util;

namespace Kafka.Client
{
    public class MessageSet : IRequestBuffer
    {
        private long messageOffset;
        private int messageSize;
        private Message message;

        public long MessageOffset
        {
            get { return messageOffset; }
            set { messageOffset = value; }
        }

        public int MessageSize
        {
            get { return messageSize; }
            set { messageSize = value; }
        }

        public Message Message
        {
            get { return message; }
            set { message = value; }
        }

        public MessageSet()
        {
        }

        public MessageSet(byte[] payLoad)
        {
            message = new Message(payLoad);
        }

        public Message SetMessage(byte[] payLoad)
        {
           message = new Message(payLoad);
           return message;
        }

        public int Parse(byte[] data, int offset )
        {
            var dataOffset = offset;
            int eaten = 0;
            dataOffset = BufferReader.Read(data, dataOffset, out messageOffset);
            dataOffset = BufferReader.Read(data, dataOffset, out messageSize);
            message = Message.ParseFrom(data.Skip(dataOffset).ToArray());
            // Return used byte count
            return messageSize + dataOffset - offset;
        }

        public List<byte> GetRequestBytes()
        {
            var requestBytes = new List<byte>();
            requestBytes.AddRange(BitWorks.GetBytesReversed(messageOffset)); // Don't care
            
            var messageBuffer = new List<byte>();
            messageBuffer.AddRange(message.GetBytes());
            requestBytes.AddRange(BitWorks.GetBytesReversed(messageBuffer.Count));
            requestBytes.AddRange(messageBuffer);
            return requestBytes;
        }
    }
}
