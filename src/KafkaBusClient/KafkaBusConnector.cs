using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using Kafka.Client;
using Kafka.Client.Response;
using KafkaBusClient.Exception;
using KafkaException = KafkaBusClient.Exception.KafkaException;

namespace KafkaBusClient
{
    public class KafkaBusConnector
    {
        private readonly Connector connector;
        private readonly String clientId;
        private const int DefaultCorrelationId = 0;
        private readonly Dictionary<string, int> topicPartitionDictionary = new Dictionary<string, int>(); 

        public KafkaBusConnector(string brookerAddress, int brookerPort, string clientId)
        {
            this.clientId = clientId;
            connector = new Connector(brookerAddress, brookerPort);
        }

        public KafkaMessageStream CreateMessageStream(string topicName, int partitionId,
                                                      KafkaMessageStream.StreamStart streamStart)
        {
            KafkaTopic kafkaTopic;
            if (streamStart == KafkaMessageStream.StreamStart.Beginning)
                kafkaTopic = CreateTopic(topicName, partitionId);
            else if (streamStart == KafkaMessageStream.StreamStart.Next)
                kafkaTopic = RecreateTopic(topicName, partitionId);
            else
                throw new ArgumentException("Invalid value for stream start.");
            
            return new KafkaMessageStream(kafkaTopic, this);
        }

        public KafkaMessageStream CreateMessageStream(string topicName, int partitionId, long offset )
        {
            var kafkaTopic = new KafkaTopic(topicName, partitionId, offset);
            return new KafkaMessageStream(kafkaTopic, this);
        }

        public List<KafkaMessage> LoadMessages(KafkaTopic kafkaTopic)
        {
            var kafkaMessages = new List<KafkaMessage>();

            var fetchResponse = connector.Fetch(kafkaTopic.TopicName, kafkaTopic.PartitionId, DefaultCorrelationId, clientId, kafkaTopic.Offset, 5000);
            foreach (var fetchTopic in fetchResponse.Topics)
            {
                foreach (var fetchPartition in fetchTopic.Partitions)
                {
                    if (fetchPartition.MessageSets == null)
                        return kafkaMessages;
                    foreach (var messageSet in fetchPartition.MessageSets)
                    {
                        var kafkaMessage = new KafkaMessage(fetchPartition.PartitionId, fetchTopic.TopicName,
                                                                     messageSet.Message.Payload,
                                                                     messageSet.MessageOffset);
                        kafkaMessages.Add(kafkaMessage);
                        kafkaTopic.Offset = kafkaMessage.MessageOffset;
                    }
                }
            }
            return kafkaMessages;
        }

        public short Produce(string topicName, int partitionId, string data)
        {
            try
            {
                if (!topicPartitionDictionary.ContainsKey(topicName))
                {
                    // Check if topic exist and on what partition. 
                    // This call will automatically create the topic if the brooker is set up to auto create
                    MetadataResponse metadataResponse = connector.Metadata(DefaultCorrelationId, clientId, topicName);
                    short errorCode = metadataResponse.TopicErrorCode(topicName);
                    if (errorCode != (short) KafkaErrorCode.NoError)
                    {
                        if (errorCode != (short) KafkaErrorCode.LeaderNotAvailable)
                            return errorCode;
                        // Check if the topic was auto created
                        metadataResponse = connector.Metadata(DefaultCorrelationId, clientId, topicName);
                        errorCode = metadataResponse.TopicErrorCode(topicName);
                        if (errorCode != (short) KafkaErrorCode.NoError)
                            return errorCode;
                        topicPartitionDictionary.Add(topicName, metadataResponse.Partitions(topicName)[0]);
                    }
                    else
                        topicPartitionDictionary.Add(topicName, metadataResponse.Partitions(topicName)[0]);
                }

                if (partitionId == -1)
                    partitionId = topicPartitionDictionary[topicName];
                var message = Encoding.UTF8.GetBytes(data);

                ProduceResponse response = connector.Produce(DefaultCorrelationId, clientId, 500, topicName, partitionId, message);
                return response.ErrorCode(topicName, 0);

            }
            catch (SocketException ex)
            {
                throw new KafkaException(ex.Message);
            }
        }

        private KafkaTopic CreateTopic(string topicName, int partitionId)
        {
            var offsetResponse = connector.GetStartOffset(topicName, clientId, partitionId);
            return TopicFromResponse(offsetResponse);
        }

        private KafkaTopic RecreateTopic(string topicName, int partitionId)
        {
            var offsetResponse = connector.GetCurrentOffset(topicName, clientId, partitionId);
            return TopicFromResponse(offsetResponse);
        }

        private KafkaTopic TopicFromResponse(OffsetResponse offsetResponse)
        {
            string topicName = offsetResponse.Topics()[0];
            int partitionId = offsetResponse.Partitions(topicName)[0];
            short errorCode = offsetResponse.Errorcode(topicName, partitionId);
            if ( errorCode != 0)
            {
                if (errorCode == 3)
                    throw new NoTopicException(topicName);
                throw new KafkaException(errorCode);
            }
            long offset = offsetResponse.Offsets(topicName, partitionId)[0];
            var kafkaTopic = new KafkaTopic(topicName, partitionId, offset);
            return kafkaTopic;
        }
    }
}
