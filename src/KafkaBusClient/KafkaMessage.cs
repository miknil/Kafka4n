using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaBusClient
{
    public class KafkaMessage
    {
        public int PartitionId { get; private set; }
        public string TopicName { get; private set; }
        public byte[] MessageData { get; private set; }
        public long MessageOffset { get; private set; }

        public KafkaMessage(int partitionId, string topicName, byte[] messageData, long messageOffset)
        {
            PartitionId = partitionId;
            TopicName = topicName;
            MessageData = messageData;
            MessageOffset = messageOffset;
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(MessageData);
        }
    }
}
