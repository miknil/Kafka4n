using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class OffsetPartition : IRequestBuffer
    {
        public int MaxNuberOfOffsets { get; set; }
        public long Time { get; set; }

        private readonly Partition partition;

        public OffsetPartition(int partitionId, long time, int maxNuberOfOffsets)
        {
            partition = new Partition(partitionId);
            Time = time;
            MaxNuberOfOffsets = maxNuberOfOffsets;
        }

        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            request.AddRange(partition.GetRequestBytes());
            request.AddRange(BitWorks.GetBytesReversed(Time));
            request.AddRange(BitWorks.GetBytesReversed(MaxNuberOfOffsets));
            return request;
        }
    }
}
