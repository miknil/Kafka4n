using System.Collections.Generic;
using System.Globalization;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class PartitionMetadata
    {
        private short partitionErrorCode;
        private int partitionId;
        private int leader;
        //  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        //  PartitionErrorCode => int16
        //  PartitionId => int32
        //  Leader => int32
        //  Replicas => [int32]
        //  Isr => [int32]

        public short PartitionErrorCode
        {
            get { return partitionErrorCode; }
            set { partitionErrorCode = value; }
        }

        public int PartitionId
        {
            get { return partitionId; }
            set { partitionId = value; }
        }

        public int Leader
        {
            get { return leader; }
            set { leader = value; }
        }

        public List<int> Replicas { get; set; }
        public List<int> Irs { get; set; }


        public int Parse(byte[] data, int dataOffset)
        {
            Replicas = new List<int>();
            int bufferOffset = dataOffset;
            bufferOffset = BufferReader.Read(data, bufferOffset, out partitionErrorCode);
            bufferOffset = BufferReader.Read(data, bufferOffset, out partitionId);
            bufferOffset = BufferReader.Read(data, bufferOffset, out leader);
            int replicaCount;
            bufferOffset = BufferReader.Read(data, bufferOffset, out replicaCount);
            for (var i = 0; i < replicaCount; i++)
            {
                int replicaId;
                bufferOffset = BufferReader.Read(data, bufferOffset, out replicaId);
                Replicas.Add(replicaId);
            }

            int irsCount;
            bufferOffset = BufferReader.Read(data, bufferOffset, out irsCount);
            Irs = new List<int>();
            for (var i = 0; i < irsCount; i++)
            {
                int irsId;
                bufferOffset = BufferReader.Read(data, bufferOffset, out irsId);
                Irs.Add(irsId);
            }
            return bufferOffset;
        }

        public override string ToString()
        {
            return partitionId.ToString(CultureInfo.InvariantCulture);
        }
    }
}
