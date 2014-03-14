using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client.Request
{
    public class ProduceTopic : IRequestBuffer
    {
        private readonly Topic topic;


        public ProduceTopic(string topicName)
        {
            topic = new Topic(topicName);
        }

        public ProducePartition AddPartition(int partitionId)
        {
            var partition = new ProducePartition(partitionId);
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
