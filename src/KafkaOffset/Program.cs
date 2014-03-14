using System;
using Kafka.Client;
using Kafka.Client.Response;

namespace KafkaOffset
{
    class Program
    {
        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("KafkaMetadata serverip:serverport topicname");
        }

        static void Main(string[] args)
        {
            string topicName = "kafkatopic";
            const int partitionId = 0;
            const int correlationId = 0;
            const int max = 4;

            if (args.Length < 1)
            {
                Usage();
                return;
            }

            string serverAddress = args[0].Split(':')[0];
            int serverPort = Convert.ToInt32(args[0].Split(':')[1]);

            if (args.Length > 1)
                topicName = args[1];


            var connector = new Connector(serverAddress, serverPort);


            // Can't get the time to work. Always use -1 i.e. from start
            const long time = -1;
            const string clientId = "c# KafkaOffset util";
            OffsetResponse offsetResponse = connector.GetOffsetResponse(topicName, time, max, correlationId, clientId, partitionId);

            foreach (var offsetTopicName in offsetResponse.Topics())
            {
                Console.WriteLine("Topic:\t\t" + offsetTopicName);
                foreach (var partition in offsetResponse.Partitions(offsetTopicName))
                {
                    Console.WriteLine("PartitionId:\t" + partition);
                    Console.WriteLine("Error:\t\t" + offsetResponse.Errorcode(offsetTopicName,partition));
                    Console.WriteLine("Offset count:\t" + offsetResponse.Offsets(offsetTopicName, partition).Count);
                    Console.Write("Offsets:\t");
                    for (int index = 0; index < offsetResponse.Offsets(offsetTopicName, partition).Count; index++)
                    {
                        if (index > 0)
                            Console.Write(',');
                        var offset = offsetResponse.Offsets(offsetTopicName, partition)[index];
                        Console.Write(offset);
                    }
                }
            }
        }
    }
}
