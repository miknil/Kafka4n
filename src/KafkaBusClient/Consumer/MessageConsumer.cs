using System;
using System.Threading;

namespace KafkaBusClient.Consumer
{
    public class MessageConsumer : IKafkaMessageConsumer
    {
        private Worker worker;
        /// <summary>
        /// Start a consumer thread on a topic that send messages to handler
        /// </summary>
        /// <param name="busConnector">Connection to a Kafka bus</param>
        /// <param name="topicName">Name of topict to fetch</param>
        /// <param name="partitionId">Partition to fetch</param>
        /// <param name="startOffset">Office to start fetch from. 0 = From first message. -1 = Next message >0 = At offset </param>
        /// <param name="consumeDelegate"></param>
        public void Start(KafkaBusConnector busConnector, string topicName, int partitionId, long startOffset, ConsumeDelegate consumeDelegate)
        {
            KafkaMessageStream messageStream;
            if (startOffset == 0)
                messageStream = busConnector.CreateMessageStream(topicName, partitionId, KafkaMessageStream.StreamStart.Beginning);
            else if (startOffset < 0)
                messageStream = busConnector.CreateMessageStream(topicName, partitionId,
                                                                 KafkaMessageStream.StreamStart.Next);
            else
                messageStream = busConnector.CreateMessageStream(topicName, partitionId, startOffset);

            worker = new Worker(messageStream, consumeDelegate);
            var workerThread = new Thread(worker.ConsumeMessages);
            workerThread.Start();
        }

        public void Pause()
        {
            throw new NotImplementedException();
        }

        public void Resume()
        {
            throw new NotImplementedException();
        }

        public void Stop()
        {
            worker.Cancel();
        }

        internal class Worker
        {
            private bool cancel;
            private readonly KafkaMessageStream messageStream;
            private readonly ConsumeDelegate handler;

            internal Worker(KafkaMessageStream messageStream, ConsumeDelegate handler)
            {
                this.messageStream = messageStream;
                this.handler = handler;
            }

            internal void Cancel()
            {
                messageStream.Close();
                cancel = true;
            }

            public void ConsumeMessages()
            {
                foreach (var message in messageStream)
                {
                    if (cancel)
                    {
                        messageStream.Close();
                        break;
                    }
                    if (handler != null)
                        handler.Invoke(message);
                }
            }
        }
    }
}
