using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class Partition : IRequestBuffer
    {

        private readonly int partitionId;

        public Partition(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed(partitionId));
            return request;
        }
    }
}
