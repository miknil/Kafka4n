using System;
using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a fetch request to send to Kafka.
    /// </summary>
    public class FetchRequest : IRequestBuffer
    {
        protected static readonly int DefaultMaxSize = 1048576;
        protected static readonly int DefaultMaxWaitTime = 500;
        protected static readonly int DefaultMinBytes = 500;

        private readonly Request request;
        private List<IRequestBuffer> topics = new List<IRequestBuffer>();

        public int MaxWaitTime { get; set; }
        public int MinBytes { get; set; }
        public List<IRequestBuffer> Topics
        {
            get { return topics; }
            set { topics = value; }
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        public FetchRequest(int correlationId, string clientId)
            :this(correlationId, clientId, DefaultMaxWaitTime, DefaultMinBytes)
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        public FetchRequest(int correlationId, string clientId, int maxWaitTime, int minBytes)
        {
            request = new Request(Request.Fetch, correlationId, clientId);
            MaxWaitTime = maxWaitTime;
            MinBytes = minBytes;
        }

        public FetchTopic AddTopic(String name)
        {
            var topic = new FetchTopic(name);
            Topics.Add(topic);
            return topic;
        }

        public FetchTopic AddTopic(String name, int partitionId, long fetchOffset, int maxBytes)
        {
            var topic = AddTopic(name);
            topic.AddPartition(partitionId, fetchOffset, maxBytes);
            return topic;
        }

        public List<byte> GetRequestBytes()
        {
            var requestBuffer = new List<byte>();
            // Get request base: ApiKey, ApiVersion, CorrelationId, ClientId
            requestBuffer.AddRange(request.GetRequestBytes());

            // Add Fetch request base: ReplicaId, MaxWaitTime, MinBytes
            requestBuffer.AddRange(BitWorks.GetBytesReversed(Request.ReplicaId));
            requestBuffer.AddRange(BitWorks.GetBytesReversed(MaxWaitTime));
            requestBuffer.AddRange(BitWorks.GetBytesReversed(MinBytes));

            if (Topics.Count > 0)
            {
                requestBuffer.AddRange(BitWorks.GetBytesReversed(Topics.Count));
                // Add Topic count and all topics including partitions
                foreach (var topic in Topics)
                {
                    requestBuffer.AddRange(topic.GetRequestBytes());
                }
            }

            requestBuffer.InsertRange(0, BitWorks.GetBytesReversed(Convert.ToInt32(requestBuffer.Count)));
            return requestBuffer;
        }
    }
}
