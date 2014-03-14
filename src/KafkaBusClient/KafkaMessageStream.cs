using System.Collections;
using System.Collections.Generic;

namespace KafkaBusClient
{
    public class KafkaMessageStream : IEnumerable<KafkaMessage>
    {
        private readonly List<KafkaMessage> kafkaMessages = new List<KafkaMessage>();
        private readonly KafkaTopic topic;
        private readonly KafkaBusConnector busConnector;
        private bool cancel;

        public enum StreamStart
        {
            Beginning,
            Next
        }

        public KafkaMessageStream(KafkaTopic topic, KafkaBusConnector busConnector)
        {
            this.busConnector = busConnector;
            this.topic = topic;
            cancel = false;
        }
        public void AddMessage(KafkaMessage message)
        {
            kafkaMessages.Add(message);
        }

        public void Close()
        {
            cancel = true;
        }

        public IEnumerator<KafkaMessage> GetEnumerator()
        {
            while (kafkaMessages.Count == 0)
            {
                if (cancel)
                    break;
                kafkaMessages.AddRange(busConnector.LoadMessages(topic));
                if (kafkaMessages.Count == 0)
                    continue;
                while (kafkaMessages.Count > 0)
                {
                    if (cancel)
                        break;
                    KafkaMessage kafakMessage = kafkaMessages[0];
                    topic.Offset = kafakMessage.MessageOffset;
                    kafkaMessages.RemoveAt(0);
                    yield return kafakMessage;
                }
                topic.Offset++;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
