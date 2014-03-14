using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class BasePartition
    {
        private short errorCode;
        private int partitionId;

        public short ErrorCode
        {
            get { return errorCode; }
            set { errorCode = value; }
        }

        public int PartitionId
        {
            get { return partitionId; }
            set { partitionId = value; }
        }

        protected BasePartition(int partitionId, short errorCode)
        {
            this.partitionId = partitionId;
            this.errorCode = errorCode;
        }

        public int ParseHeaderData(byte[] data, int offset)
        {
            var dataOffset = offset;
            dataOffset = BufferReader.Read(data, dataOffset, out partitionId);
            dataOffset = BufferReader.Read(data, dataOffset, out errorCode);
            return dataOffset;
        }
    }
}
