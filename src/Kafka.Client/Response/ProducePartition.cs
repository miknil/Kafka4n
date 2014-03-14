namespace Kafka.Client.Response
{
    public class ProducePartition
    {
        public int PartitionId { get; private set; }
        public short ErrorCode { get; private set; }
        public long Offset { get; private set; }

        public ProducePartition(int partitionId, short errorCode, long offset)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            Offset = offset;
        }
    }
}
