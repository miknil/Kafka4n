using System;
using System.Collections.Generic;

namespace Kafka.Client.Request
{
    public class FetchTopic : IRequestBuffer
    {
        private readonly Topic topic;

        public FetchTopic(String topicName)
        {
            topic = new Topic(topicName);
        }

        public FetchPartition AddPartition(int partitionId, long fetchOffset, int maxBytes)
        {
            var partition = new FetchPartition(partitionId, fetchOffset, maxBytes);
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
