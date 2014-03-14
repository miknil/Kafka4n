using System;
using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class OffsetRequest : IRequestBuffer
    {

        private readonly Request request;
        private List<IRequestBuffer> topics = new List<IRequestBuffer>();

        /// <summary>
        /// The latest time constant. Specify -1 to receive the latest offsets
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant. Specify -2 to receive the earliest available offset. 
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        public OffsetRequest(int correlationId, string clientId)
        {
            request = new Request(Request.Offset, correlationId,clientId);
        }

        public OffsetTopic AddTopic(string topicName)
        {
            var topic = new OffsetTopic(topicName);
            topics.Add(topic);
            return topic;
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        /// <param name="topicName">The topic to publish to.</param>
        /// <param name="partitionId">The partition to publish to.</param>
        /// <param name="time">The time from which to request offsets.</param>
        /// <param name="maxNumberOfOffsets">The maximum amount of offsets to return.</param>
        public OffsetTopic AddTopic(string topicName, int partitionId, long time, int maxNumberOfOffsets)
        {
            var topic = AddTopic(topicName);
            topic.AddPartition(partitionId, time, maxNumberOfOffsets);
            return topic;
        }

        public List<byte> GetRequestBytes()
        {
            var requestBuffer = new List<byte>();
            // Get request base: ApiKey, ApiVersion, CorrelationId, ClientId
            requestBuffer.AddRange(request.GetRequestBytes());

            // Add Fetch request base: ReplicaId, MaxWaitTime, MinBytes
            requestBuffer.AddRange(BitWorks.GetBytesReversed(Request.ReplicaId));

            if (topics.Count > 0)
            {
                requestBuffer.AddRange(BitWorks.GetBytesReversed(topics.Count));
                // Add Topic count and all topics including partitions
                foreach (var topic in topics)
                {
                    requestBuffer.AddRange(topic.GetRequestBytes());
                }
            }

            requestBuffer.InsertRange(0, BitWorks.GetBytesReversed(Convert.ToInt32(requestBuffer.Count)));
            return requestBuffer;
        }
    }
}
