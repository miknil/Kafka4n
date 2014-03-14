using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class ProducePartition : IRequestBuffer
    {
        private int partitionId;
        private int messageSetSize;
        private MessageSet messageSet;


        public ProducePartition(int partitionId, MessageSet messageSet)
        {
            this.partitionId = partitionId;
            this.messageSet = messageSet;
        }

        public ProducePartition(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public void AddMessageSet(MessageSet set)
        {
            messageSet = set;
        }

        public List<byte> GetRequestBytes()
        {
            var requestBytes = new List<byte>();
            requestBytes.AddRange(BitWorks.GetBytesReversed(partitionId));

            var messageSetBuffer = new List<byte>();
            messageSetBuffer.AddRange(messageSet.GetRequestBytes());
            requestBytes.AddRange(BitWorks.GetBytesReversed(messageSetBuffer.Count));
            requestBytes.AddRange(messageSetBuffer);
            return requestBytes;
        }
    }
}
