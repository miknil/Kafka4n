using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class MetadataResponse
    {

  //      [Broker][TopicMetadata]

        public List<Broker> Brokers { get; set; }
        private readonly Dictionary<string,TopicMetaData> topicMetaDatas = new Dictionary<string, TopicMetaData>();
        private int correlationId;

        public int CorrelationId
        {
            get { return correlationId; }
        }


        public int Parse(byte[] data, int dataOffset)
        {
            int bufferOffset = dataOffset;
            Brokers = new List<Broker>();
            
            bufferOffset = BufferReader.Read(data, bufferOffset, out correlationId);
            int brokerCount;
            bufferOffset = BufferReader.Read(data, bufferOffset, out brokerCount);
            for (var i = 0; i < brokerCount; i++)
            {
                Broker broker = new Broker();
                bufferOffset = broker.Parse(data, bufferOffset);
                Brokers.Add(broker);
            }

            int topicCount;
            bufferOffset = BufferReader.Read(data, bufferOffset, out topicCount);
            for (var i = 0; i < topicCount; i++)
            {
                TopicMetaData topicMetaData = new TopicMetaData();
                bufferOffset = topicMetaData.Parse(data, bufferOffset);
                topicMetaDatas.Add(topicMetaData.TopicName,topicMetaData);
            }

            return bufferOffset;
        }

        public List<string> Topics()
        {
            return new List<string>(topicMetaDatas.Keys);
        } 

        public short TopicErrorCode(string topicName)
        {
            return topicMetaDatas[topicName].TopicErrorCode;
        }

        public short PartitionErrorCode(string topicName, int partitionId)
        {
            return topicMetaDatas[topicName].TopicErrorCode;
        }

        public List<int> Partitions(string topicName)
        {
            List<int> partitions = new List<int>();
            if (!topicMetaDatas.ContainsKey(topicName))
                return partitions;

            TopicMetaData topicMetaData = topicMetaDatas[topicName];

            return topicMetaData.Partitions();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("Topics:");
            foreach(TopicMetaData topicMetaData in topicMetaDatas.Values)
            {
                foreach (var partitionId in topicMetaData.Partitions())
                {
                    PartitionMetadata partitionMetaData = topicMetaData.Partition(partitionId);
                    sb.Append("\tname: ").Append(topicMetaData.TopicName).Append('\t');
                    sb.Append("partition: ").Append(partitionMetaData.PartitionId).Append('\t');
                    sb.Append("leader: ").Append(partitionMetaData.Leader).Append('\t');
                    sb.Append("replicas: ");
                    for (int index = 0; index < partitionMetaData.Replicas.Count; index++)
                    {
                        if (index != 0)
                            sb.Append(',');
                        sb.Append(partitionMetaData.Replicas[index]);
                    }
                    sb.Append("\tirs: ");
                    for (int index = 0; index < partitionMetaData.Irs.Count; index++)
                    {
                        if (index != 0)
                            sb.Append(',');
                        sb.Append(partitionMetaData.Irs[index]);
                    }
                    sb.AppendLine();
                }
            }
            return sb.ToString();
        }
    }
}
