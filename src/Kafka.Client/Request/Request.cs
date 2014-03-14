using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class Request : IRequestBuffer
    {

        public const short Produce = 0;
        public const short Fetch = 1;
        public const short Offset = 2;
        public const short Metadata = 3;

        public const int ReplicaId = -1;


        protected static readonly short ApiVersion = 0;

        protected short ApiKey;
        private readonly short apiVersion;
        protected int CorrelationId;
        protected String ClientId;


        /// <summary>
        /// Initializes a new instance of the Request class.
        /// </summary>
        public Request(short apiKey,int correlationId, string clientId)
        {
            ApiKey = apiKey;
            CorrelationId = correlationId;
            ClientId = clientId;
            apiVersion = ApiVersion;
        }

        /// <summary>
        /// Converts the request to an array of bytes that is expected by Kafka.
        /// </summary>
        /// <returns>A list of bytes that represents the request.</returns>
        public List<byte> GetBytes()
        {
            var request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed(ApiKey));
            request.AddRange(BitWorks.GetBytesReversed(apiVersion));
            request.AddRange(BitWorks.GetBytesReversed(CorrelationId));
            return request;
        }

        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed(ApiKey));
            request.AddRange(BitWorks.GetBytesReversed(apiVersion));
            request.AddRange(BitWorks.GetBytesReversed(CorrelationId));
            request.AddRange(BitWorks.GetBytesReversed((short)ClientId.Length));
            request.AddRange(Encoding.ASCII.GetBytes(ClientId));
            return request;
        }
    }
}
