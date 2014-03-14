using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaBusClient
{
    public delegate void ConsumeDelegate(KafkaMessage message);
    public interface IKafkaMessageConsumer
    {
        void Start(KafkaBusConnector busConnector, string topicName, int partitionId, long startOffset, ConsumeDelegate consumeDelegate);
        void Pause();
        void Resume();
        void Stop();
    }
}
