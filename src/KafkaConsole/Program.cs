using System;
using System.Text;
using System.Threading;
using Kafka.Client;
using Kafka.Client.Request;
using Kafka.Client.Response;
using KafkaBusClient;
using KafkaBusClient.Consumer;
using FetchPartition = Kafka.Client.Response.FetchPartition;

namespace KafkaConsole
{
    class Program
    {

        static void DumpMessage(KafkaMessage message)
        {
            Console.WriteLine(message);
        }

        static void Main(string[] args)
        {
            string topicName = "kafkatopic";
            int partitionId = 0;
            int correlationId = 0;


            KafkaBusConnector busConnector = new KafkaBusConnector("192.168.0.105", 9092, "KafkaConsole");

            IKafkaMessageConsumer consumer = new MessageConsumer();
            consumer.Start(busConnector, topicName, partitionId, 4, DumpMessage);

            while (Console.KeyAvailable == false)
            {
                Thread.Sleep(100);
            }
            consumer.Stop();

            //short errorCode = busConnector.Produce(topicName, -1, "Hello");

            var stream = busConnector.CreateMessageStream(topicName, partitionId, KafkaMessageStream.StreamStart.Beginning);
            foreach (var kafkaMessage in stream)
            {
                Console.WriteLine(kafkaMessage);
                if (Console.KeyAvailable == true)
                {
                    ConsoleKeyInfo info = Console.ReadKey();
                    stream.Close();
                }
            }
            stream = busConnector.CreateMessageStream(topicName, partitionId, KafkaMessageStream.StreamStart.Next);
            foreach (var kafkaMessage in stream)
            {
                Console.WriteLine(kafkaMessage);
                if (Console.KeyAvailable == true)
                {
                    ConsoleKeyInfo info = Console.ReadKey();
                    stream.Close();
                }
            }



            Connector connector = new Connector("192.168.0.105", 9092);
            int max = 4;

            
            byte[] message = new byte[]{49};
            message = Encoding.ASCII.GetBytes("Hello from c#");
            //MetadataResponse metadataResponse = connector.Metadata(correlationId, "C#", topicName);

            //Console.WriteLine(metadataResponse);

            //foreach (var broker in metadataResponse.Brokers)
            //{
            //    Console.WriteLine("Brooker:" + broker);
            //}

            //foreach (string metadataTopicName in metadataResponse.Topics())
            //{
            //    Console.Write("Topic:" + metadataTopicName);

            //    if (metadataResponse.TopicErrorCode(metadataTopicName) != 0)
            //        Console.WriteLine("\tTopic error:" + metadataResponse.TopicErrorCode(metadataTopicName));
            //    else
            //        Console.WriteLine();

            //    foreach (var partitionMetadata in metadataResponse.Partitions(topicName))
            //    {
            //        Console.WriteLine("\t" + partitionMetadata);
                    
            //    }
            //}

            //connector.Produce(correlationId, "c#", 500, topicName, partitionId, message);


            OffsetResponse offsetResponse = connector.GetOffsetResponse(topicName, OffsetRequest.LatestTime, max, correlationId, "C#", partitionId);

            FetchResponse fetchResponse;
            int maxOffset = 0;
            foreach (var offsetTopicName in offsetResponse.Topics())
            {
                Console.WriteLine(offsetTopicName);
                foreach (var partition in offsetResponse.Partitions(offsetTopicName))
                {
                    Console.WriteLine("PartitionId:" + partition);
                    Console.WriteLine("Error:" + offsetResponse.Errorcode(offsetTopicName,partition));
                    Console.WriteLine("Offset count:" + offsetResponse.Offsets(offsetTopicName, partition).Count);
                    Console.WriteLine("Start offset:" + offsetResponse.Offsets(offsetTopicName, partition)[1]);
                    Console.WriteLine("End offset:" + offsetResponse.Offsets(offsetTopicName, partition)[0]);
                    fetchResponse = connector.Fetch(offsetTopicName, partition, correlationId, "C#", offsetResponse.Offsets(offsetTopicName, partition)[1],5000);

                    foreach (var fetchTopic in fetchResponse.Topics)
                    {
                        foreach (FetchPartition fetchPartition in fetchTopic.Partitions)
                        {
                            Console.WriteLine("Error:" + fetchPartition.ErrorCode);
                            Console.WriteLine("Id:" + fetchPartition.PartitionId);
                            foreach (var messageSet in fetchPartition.MessageSets)
                            {
                                if (messageSet != null)
                                {
                                    Console.Write("Offset:" + messageSet.MessageOffset + "\t");
                                    Console.WriteLine("Message:" + messageSet.Message);
                                }
                            }
                            Console.WriteLine();
                        }
                    }
                }
            }

            //fetchResponse = connector.Fetch("kafkatopic", 0, 1, "C#", 7);
            //foreach (var fetchTopic in fetchResponse.Topics)
            //{
            //    foreach (FetchPartition fetchPartition in fetchTopic.Partitions)
            //    {
            //        Console.WriteLine("Error:" + fetchPartition.ErrorCode);
            //        Console.WriteLine("Id:" + fetchPartition.PartitionId);
            //        if (fetchPartition.MessageSet != null)
            //        {
            //            Console.WriteLine(fetchPartition.MessageSet.Message);
            //        }
            //        Console.WriteLine();
            //    }
            //}


            //FetchResponse fetchResponse = connector.Fetch("kafkatopic", partitionId, correlationId, "C#", offsetResponse.Topics[0].Partitions[0].Offsets[1]);

            //foreach (var topic in fetchResponse.Topics)
            //{
            //    Console.WriteLine(topic.TopicName);
            //    foreach (FetchPartition partition in topic.Partitions)
            //    {
            //        Console.WriteLine("Error:" + partition.ErrorCode);
            //        Console.WriteLine("Id:" + partition.PartitionId);
            //        Console.Write("Offset:" + partition.MessageSet.MessageOffset + " ");
            //        Console.WriteLine(partition.MessageSet.Message);
            //    }
            //}



            //FetchResponse fetchResponse = connector.Fetch("kafkatopic", 0, 1, 1, offsetResponse.Topics[0].Partitions[0].Offsets[0]);
            //long[] offsets = consumer.GetOffsetsBefore("kafkatopic", 0, OffsetRequest.LatestTime, max).ToArray();
            //List<Message> messages = consumer.Consume("kafkatopic", 0, offsets[0]);
            //foreach (var message in messages)
            //{
            //    Console.WriteLine(message.ToString());
            //}
        }
    }
}
