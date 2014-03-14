using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class FetchPartition : IRequestBuffer
    {

        private readonly Partition partition;

        public long FetchOffset { get; set; }
        public int MaxBytes { get; set; }

        public FetchPartition(int partitionId, long fetchOffset, int maxBytes)
        {
            partition = new Partition(partitionId);
            FetchOffset = fetchOffset;
            MaxBytes = maxBytes;
        }

        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            request.AddRange(partition.GetRequestBytes());
            request.AddRange(BitWorks.GetBytesReversed(FetchOffset));
            request.AddRange(BitWorks.GetBytesReversed(MaxBytes));
            return request;
        }
    }
}
