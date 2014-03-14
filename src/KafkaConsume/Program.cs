using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using KafkaBusClient;
using KafkaBusClient.Consumer;

namespace KafkaConsume
{
    class Program
    {
        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("KafkaMetadata serverip:serverport topic name [startoffset]");
        }
        static void DumpMessage(KafkaMessage message)
        {
            Console.WriteLine(message);
        }
        static void Main(string[] args)
        {
            const int partitionId = 0;

            if (args.Length < 2)
            {
                Usage();
                return;
            }

            var serverAddress = args[0].Split(':')[0];
            var serverPort = Convert.ToInt32(args[0].Split(':')[1]);
            var topicName = args[1];
            long startOffset = 5;
            if (args.Length > 2)
                startOffset = Convert.ToInt64(args[2]);

            var busConnector = new KafkaBusConnector(serverAddress, serverPort, "c# KafkaConsume util");

            IKafkaMessageConsumer consumer = new MessageConsumer();
            consumer.Start(busConnector, topicName, partitionId, startOffset, DumpMessage);

            while (Console.KeyAvailable == false)
            {
                Thread.Sleep(100);
            }
            consumer.Stop();
            
        }
    }
}
