using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class FetchPartition : BasePartition
    {
        private long highwaterMarkOffset;
        private int messageSetSize;
        public List<MessageSet> MessageSets { get; set; }

        public long HighwaterMarkOffset
        {
            get { return highwaterMarkOffset; }
            set { highwaterMarkOffset = value; }
        }

        public int MessageSetSize
        {
            get { return messageSetSize; }
            set { messageSetSize = value; }
        }

        public FetchPartition(int partitionId, short errorCode)
            : base(partitionId, errorCode){}

        public int Parse(byte[] data, int dataOffset)
        {
            dataOffset = BufferReader.Read(data, dataOffset, out highwaterMarkOffset);
            dataOffset = BufferReader.Read(data, dataOffset, out messageSetSize);
            if (messageSetSize == 0)
                return dataOffset;
            MessageSets = new List<MessageSet>();
            int eaten = 0;
            int consumed = 0;
            try
            {
                while (consumed < messageSetSize)
                {
                    MessageSet messageSet = new MessageSet();
                    eaten = messageSet.Parse(data, dataOffset);
                    if (eaten == -1)
                        break;
                    MessageSets.Add(messageSet);
                    consumed += eaten;
                    dataOffset += eaten;
                }
                return dataOffset;
            }
            catch (ArgumentException ex)
            {
                return data.Count();
            }
            catch (IndexOutOfRangeException ex)
            {
                return data.Count();
            }
        }
    }
}
