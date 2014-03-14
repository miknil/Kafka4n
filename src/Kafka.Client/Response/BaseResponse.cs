using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class BaseResponse
    {
        private int correlationId;
        protected int TopicCount;

        public int CorrelationId
        {
            get { return correlationId; }
            set { correlationId = value; }
        }

        protected int ParseHeaderData(byte[] data)
        {
            var dataOffset = BufferReader.Read(data, 0, out correlationId);
            dataOffset = BufferReader.Read(data, dataOffset, out TopicCount);
            return dataOffset;
        }
    }
}
