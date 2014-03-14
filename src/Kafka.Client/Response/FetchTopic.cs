using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class FetchTopic : BaseTopic
    {
        public List<FetchPartition> Partitions { get; set; }
        public FetchTopic(string topicName)
            :base(topicName)
        {
        }

        public int Parse(byte[] data, int dataIndex)
        {
            int bufferOffset = ParseHeaderData(data, dataIndex);
            Partitions = new List<FetchPartition>(PartitionCount);

            for (var i = 0; i < PartitionCount; i++)
            {
                int partitionId;
                short errorCode;
                bufferOffset = BufferReader.Read(data, bufferOffset, out partitionId);
                bufferOffset = BufferReader.Read(data, bufferOffset, out errorCode);
                var fetchPartition = new FetchPartition(partitionId, errorCode);
                Partitions.Add(fetchPartition);
                bufferOffset = fetchPartition.Parse(data, bufferOffset);
                if (bufferOffset >= data.Length)
                    break;
            }
            return bufferOffset;
        }
    }
}
