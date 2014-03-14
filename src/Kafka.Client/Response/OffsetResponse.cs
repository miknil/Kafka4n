using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class OffsetResponse
    {
        private readonly int correlationId;

        public int CorrelationId
        {
            get { return correlationId; }
        }

        private readonly Dictionary<string, List<OffsetPartition>> offsetPartitions = new Dictionary<string, List<OffsetPartition>>();

        public OffsetResponse(byte[] data)
        {
            var dataOffset = BufferReader.Read(data, 0, out correlationId);

            int numTopics;
            dataOffset = BufferReader.Read(data, dataOffset, out numTopics);

            for (int topicIndex = 0; topicIndex < numTopics; topicIndex++)
            {
                String topicName;
                dataOffset = BufferReader.Read(data, dataOffset, out topicName);

                int partitionCount;
                dataOffset = BufferReader.Read(data, dataOffset, out partitionCount);
                var offsetPartitionList = new List<OffsetPartition>(partitionCount);

                for (var i = 0; i < partitionCount; i++)
                {
                    int partitionId;
                    dataOffset = BufferReader.Read(data, dataOffset, out partitionId);

                    short errorCode;
                    dataOffset = BufferReader.Read(data, dataOffset, out errorCode);

                    int offsetCount;
                    dataOffset = BufferReader.Read(data, dataOffset, out offsetCount);
                    var partition = new OffsetPartition(errorCode, partitionId);
                    for (var offsetIndex = 0; offsetIndex < offsetCount; offsetIndex++)
                    {
                        long offset;
                        dataOffset = BufferReader.Read(data, dataOffset, out offset);
                        partition.Add(offset);
                    }
                    offsetPartitionList.Add(partition);
                }
                offsetPartitions.Add(topicName, offsetPartitionList);
            }
        }

        public List<long> Offsets(string topicName, int partitionId)
        {
            return offsetPartitions[topicName][partitionId].Offsets;
        }
        public short Errorcode(string topicName, int partitionId)
        {
            return offsetPartitions[topicName][partitionId].ErrorCode;
        }
        public List<string> Topics()
        {
            return new List<string>(offsetPartitions.Keys);
        }

        public List<int> Partitions(string topicName)
        {
            return offsetPartitions[topicName].Select(offsetPartition => offsetPartition.PartitionId).ToList();
        }
    }
}
