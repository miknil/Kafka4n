namespace KafkaBusClient
{
    public class KafkaTopic
    {
        public string TopicName { get; private set; }
        public int PartitionId { get; private set; }
        public long Offset { get; set; }
        public KafkaMessageStream KafkaMessageStream { get; set; }

        public KafkaTopic(string topicName, int partitionId, long offset)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            Offset = offset;
        }

        public override bool Equals(object obj)
        {
            var other = obj as KafkaTopic;
            if (other == null)
                return false;
            
            if (System.String.Compare(other.TopicName, TopicName, System.StringComparison.Ordinal) == 0
                && other.PartitionId == PartitionId)
                return true;
            return false;
        }
    }
}
