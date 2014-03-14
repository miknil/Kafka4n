using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public abstract class BaseTopic
    {
        protected int PartitionCount;
        private string topicName;
        public string TopicName
        {
            get { return topicName; }
            set { topicName = value; }
        }

        protected BaseTopic(string topicName)
        {
            this.topicName = topicName;
        }

        protected int ParseHeaderData(byte[] data, int dataIndex)
        {
            int bufferOffset = BufferReader.Read(data, dataIndex, out PartitionCount);
            return bufferOffset;

        }
    }
}
