#Kafka4n #
I was not able to find any C# client for the 0.8 version of the Apache Kafka bus so I decided
to write one. Its based on the .NET Kafka Client here https://github.com/kafka-dev/kafka/blob/master/clients/csharp
There are a lot of changes made to the code because of the difference in the protocol between 0.7 and 0.8
The project is in an very early state at the moment and I hope I will get time to put more work into it soon

I started this project because I wanted to explore the concept of µ-services and needed a C# client. The code is
used in one of my hobby projects at the moment and is not considered 'industrial strength' with full exception handling
and test coverage.

## API ##
###Low level ###
The Connector object provide the lowest level of access to the Kafka bus. It contains basic methods for querying meta data and topic offset, fetch data and producing data. The constructor takes the broker ip address and port number as input.
See Kafka [Kafka protocol wiki](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol "wiki") for a explanation of the different data structures.
#### Metadata ####
The meta data method provide meta data for one or more topics. The method return meta data for all topics when the topic name is null.

`public MetadataResponse Metadata(int correlationId, string clientId, String topicName)`
	
The MetadataResponse contains a list of brokers and meta data for each requested topic.
#### GetOffsetResponse ####
The GetOffsetResopnse method returns the log offset for one or more topic/partition combinations. I'm no sure how the maxNumOffsets parameter is supposed to be used. The request return either 1 parameter containing next message offset or 2 parameters containing first and next message offset.

    public OffsetResponse GetOffsetResponse(string topic, long time, int maxNumOffsets, int correlationId, string clientId, int partitionId)
#### Fetch ####
The Fetch method is used to get messages on one or more topic/partition combinations. The offset parameter sets the start offset for the fetch operation and the method will block if there are no messages at this offset. The method will timeout after a while. The maxByte parameter controls the size of the return buffer. This limitation can cause the last message structure to be incomplete and that message will be discarded. The offset of the last message in the response can be used in next request to continue fetching messages. The method with the promStart parameter will always fetch messages from the beginning of the message log.

    public FetchResponse Fetch(string topic, int partition, int correlationId, string clientId, long offset, int maxBytes, bool fromStart)
    public FetchResponse Fetch(string topic, int partition, int correlationId, string clientId, long offset, int maxBytes)

#### Produce ####
The produce method take a byte buffer as payload and sen it to a topic/partition combination.
 

    public ProduceResponse Produce(int correlationId, string clientId, int timeOut, string topicName, int partitionId, byte[] payLoad)
### Usage###
The KafkaConsole program was used as work bench to exercise the low level api during development. The following code snippet will query meta data on the 'kafkatopic' topic, and create the topic if not present on the broker, send a message and the retrieve all messages on that topic.


    static void Main(string[] args)
    {
        string topicName = "kafkatopic";
        int partitionId = 0;
        int correlationId = 0;
        int max = 2;
        Connector connector = new Connector("192.168.0.105", 9092);

        MetadataResponse metadataResponse = connector.Metadata(correlationId, "C#", topicName);
        Console.WriteLine(metadataResponse);

        byte[] message = new byte[] { 49 };
        message = Encoding.ASCII.GetBytes("Hello from c#");
        connector.Produce(correlationId, "c#", 500, topicName, partitionId, message);

        OffsetResponse offsetResponse = connector.GetOffsetResponse(topicName, OffsetRequest.LatestTime, max, correlationId, "C#", partitionId);

        foreach (var offsetTopicName in offsetResponse.Topics())
        {
            Console.WriteLine(offsetTopicName);
            foreach (var partition in offsetResponse.Partitions(offsetTopicName))
            {
                Console.WriteLine("PartitionId:" + partition);
                Console.WriteLine("Error:" + offsetResponse.Errorcode(offsetTopicName, partition));
                Console.WriteLine("Offset count:" + offsetResponse.Offsets(offsetTopicName, partition).Count);
                Console.WriteLine("Start offset:" + offsetResponse.Offsets(offsetTopicName, partition)[1]);
                Console.WriteLine("End offset:" + offsetResponse.Offsets(offsetTopicName, partition)[0]);
                FetchResponse fetchResponse = connector.Fetch(offsetTopicName, partition, correlationId, "C#", offsetResponse.Offsets(offsetTopicName, partition)[1], 5000);

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
    }
