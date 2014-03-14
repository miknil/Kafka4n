using System;
using Kafka.Client;
using Kafka.Client.Response;

namespace KafkaMetadata
{
    class Program
    {
        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("KafkaMetadata serverip:serverport [topic name]");
        }

        static int Main(string[] args)
        {
            string topicName = null;
            const int correlationId = 0;

            if (args.Length < 1)
            {
                Usage();
                return -1;
            }

            string serverAddress = args[0].Split(':')[0];
            int serverPort = Convert.ToInt32(args[0].Split(':')[1]);

            if (args.Length > 1)
                topicName = args[1];

            var connector = new Connector(serverAddress, serverPort);

            MetadataResponse metadataResponse = connector.Metadata(correlationId, "C# KafkaMetadata util", topicName);

            Console.WriteLine("Brookers:");
            foreach (var broker in metadataResponse.Brokers)
            {
                Console.WriteLine("\t" + broker);
            }
            Console.WriteLine(metadataResponse);

            return 0;

        }
    }
}
