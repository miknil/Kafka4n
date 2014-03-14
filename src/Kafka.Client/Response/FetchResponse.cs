using System.Collections.Generic;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class FetchResponse : BaseResponse
    {
        public List<FetchTopic> Topics { get; set; }
        public FetchResponse(byte[] data)
        {
            var dataOffset = ParseHeaderData(data);
            Topics = new List<FetchTopic>(TopicCount);
            for (var i = 0; i < TopicCount; i++)
            {
                string topicName;
                dataOffset = BufferReader.Read(data, dataOffset, out topicName);
                var topic = new FetchTopic(topicName);
                dataOffset = topic.Parse(data, dataOffset);

                Topics.Add(topic);
            }
        }
    }
}
