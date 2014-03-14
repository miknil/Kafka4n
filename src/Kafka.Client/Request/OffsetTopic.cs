using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Response;

namespace Kafka.Client.Request
{
    public class OffsetTopic : IRequestBuffer
    {
        private readonly Topic topic;

        public OffsetTopic(String topicName)
        {
            topic = new Topic(topicName);
        }

        public OffsetPartition AddPartition(int partitionId, long time, int maxNumberOfOffsets)
        {
            var partition = new OffsetPartition(partitionId, time, maxNumberOfOffsets);
            topic.Partitions.Add(partition);
            return partition;
        }

        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            request.AddRange(topic.GetRequestBytes());
            return request;
        }

    }
}
