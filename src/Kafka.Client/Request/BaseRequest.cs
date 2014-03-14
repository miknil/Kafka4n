using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class BaseRequest
    {
        protected static readonly int DefaultMaxSize = 1048576;
        protected static readonly int DefaultMaxWaitTime = 5000;
        protected static readonly int DefaultMinBytes = 50;
        protected static readonly short ApiVersion = 0;
        protected static readonly int ReplicaId = -1;

        private readonly string clientId;

        public int CorrelationId { get; set; }

        protected string Topic;

        /// <summary>
        /// Gets or sets the partition to publish to.
        /// </summary>
        public int Partition { get; set; }

        public BaseRequest(string topic, int partition, int correlationId, string clientId)
        {
            Topic = topic;
            Partition = partition;
            this.clientId = clientId;
            CorrelationId = correlationId;
        }

        /// <summary>
        /// Converts the request to an array of bytes that is expected by Kafka.
        /// </summary>
        /// <returns>An array of bytes that represents the request.</returns>
        public List<byte> GetBytes()
        {
            var request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed((short)RequestType.Fetch));
            request.AddRange(BitWorks.GetBytesReversed(ApiVersion));
            request.AddRange(BitWorks.GetBytesReversed(CorrelationId));
            request.AddRange(BitWorks.GetBytesReversed((short)clientId.Length));
            request.AddRange(Encoding.ASCII.GetBytes(clientId));
            return request;
        }
    }
}
