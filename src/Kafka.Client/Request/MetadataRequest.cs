using System;
using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class MetadataRequest :IRequestBuffer
    {
        private List<Topic> topics = new List<Topic>();
        private readonly Request request;
        
        public MetadataRequest(int correlationId, string clientId)
        {
            request = new Request(Request.Metadata, correlationId, clientId);
        }

        public MetadataRequest(int correlationId, string clientId, String topicName)
            : this(correlationId, clientId)
        {
            if (topicName != null)
                AddTopic(topicName);
        }

        public void AddTopic(String topicName)
        {
            var topic = new Topic(topicName);
            topics.Add(topic);
        }

        public List<byte> GetRequestBytes()
        {
            var requestBuffer = new List<byte>();
            // Get request base: ApiKey, ApiVersion, CorrelationId, ClientId
            requestBuffer.AddRange(request.GetRequestBytes());
            requestBuffer.AddRange(BitWorks.GetBytesReversed(topics.Count));

            // Add Topic count and all topics including partitions
            foreach (var topic in topics)
            {
                requestBuffer.AddRange(topic.GetRequestBytes());
            }

            requestBuffer.InsertRange(0, BitWorks.GetBytesReversed(Convert.ToInt32(requestBuffer.Count)));
            return requestBuffer;
        }
    }
}
