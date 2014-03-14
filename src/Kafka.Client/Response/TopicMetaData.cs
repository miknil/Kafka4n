using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class TopicMetaData
    {
        private short topicErrorCode;
        private string topicName;

//TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
//  TopicErrorCode => int16
//  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr

        public Dictionary<int,PartitionMetadata> PartitionMetadatas { get; set; }

        public short TopicErrorCode
        {
            get { return topicErrorCode; }
            set { topicErrorCode = value; }
        }

        public string TopicName
        {
            get { return topicName; }
            set { topicName = value; }
        }

        public int Parse(byte[] data, int dataOffset)
        {
            int bufferOffset = dataOffset;
            bufferOffset = BufferReader.Read(data, bufferOffset, out topicErrorCode);
            bufferOffset = BufferReader.Read(data, bufferOffset, out topicName);
            PartitionMetadatas = new Dictionary<int, PartitionMetadata>();
            int partitionCount;
            bufferOffset = BufferReader.Read(data, bufferOffset, out partitionCount);
            for (var i = 0; i < partitionCount; i++)
            {
                PartitionMetadata metadata = new PartitionMetadata();
                bufferOffset = metadata.Parse(data, bufferOffset);
                PartitionMetadatas.Add(metadata.PartitionId, metadata);
            }
            return bufferOffset;
        }

        public List<int> Partitions()
        {
            return new List<int>(PartitionMetadatas.Keys);
        } 

        public PartitionMetadata Partition(int partitionId)
        {
            return PartitionMetadatas[partitionId];
        }

        public override string ToString()
        {
            return topicName;
        }
    }
}
