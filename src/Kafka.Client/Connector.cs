using System;
using System.Collections.Generic;
using Kafka.Client.Request;
using Kafka.Client.Response;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Consumes messages from Kafka.
    /// </summary>
    public class Connector
    {
        private readonly string server;
        private readonly int port;

        private const int DefaultCorrelationId = 0;

        /// <summary>
        /// Initializes a new instance of the Consumer class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public Connector(string server, int port)
        {
            this.server = server;
            this.port = port;
        }

        public FetchResponse Fetch(string topic, int partition, int correlationId, string clientId, long offset, int maxBytes, bool fromStart)
        {
            if (fromStart)
                offset = OffsetRequest.LatestTime;
            FetchRequest fetchRequest = new FetchRequest(correlationId, clientId);
            fetchRequest.AddTopic(topic, partition, offset, maxBytes);
            return Fetch(fetchRequest);
        }

        public FetchResponse Fetch(string topic, int partition, int correlationId, string clientId, long offset, int maxBytes)
        {
            FetchRequest fetchRequest = new FetchRequest(correlationId, clientId);
            fetchRequest.AddTopic(topic, partition, offset, maxBytes);
            return Fetch(fetchRequest);
        }

        public FetchResponse Fetch(FetchRequest request)
        {
            using (KafkaConnection connection = new KafkaConnection(server, port))
            {
                connection.Write(request.GetRequestBytes().ToArray());
                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);
                if (dataLength > 0)
                {
                    byte[] data = connection.Read(dataLength);
                    var fetchResponse = new FetchResponse(data);
                    return fetchResponse;
                }
                return null;
            }
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="topic">The topic to check.</param>
        /// <param name="time">time in millisecs (if -1, just get from the latest available)</param>
        /// <param name="maxNumOffsets">That maximum number of offsets to return.</param>
        /// <param name="correlationId"></param>Id used by the client to identify this transaction. Returned in the response
        /// <param name="clientId"></param>Name to identify the client. Used in server logs
        /// <param name="partitionId">The partition on the topic.</param>
        /// <returns>OffseRersponse</returns>
        public OffsetResponse GetOffsetResponse(string topic, long time, int maxNumOffsets, int correlationId, string clientId, int partitionId)
        {
            var request = new OffsetRequest(correlationId, clientId);
            request.AddTopic(topic, partitionId, time, maxNumOffsets);
            return GetOffsetResponseBefore(request);
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="topic">The topic to check.</param>
        /// <param name="clientId"></param>Name to identify the client. Used in server logs
        /// <param name="partitionId">The partition on the topic.</param>
        /// <returns>OffseRersponse containing the first offser for the topic</returns>
        public OffsetResponse GetStartOffset(string topic, string clientId, int partitionId)
        {
            var request = new OffsetRequest(DefaultCorrelationId, clientId);
            request.AddTopic(topic, partitionId, OffsetRequest.EarliestTime, 1);
            return GetOffsetResponseBefore(request);
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="topic">The topic to check.</param>
        /// <param name="clientId"></param>Name to identify the client. Used in server logs
        /// <param name="partitionId">The partition on the topic.</param>
        /// <returns>OffseRersponse containing the next offser for the topic</returns>
        public OffsetResponse GetCurrentOffset(string topic, string clientId, int partitionId)
        {
            var request = new OffsetRequest(DefaultCorrelationId, clientId);
            request.AddTopic(topic, partitionId, OffsetRequest.LatestTime, 1);
            return GetOffsetResponseBefore(request);
        }
        /// <summary>
        /// Create an OffsetRequest to get the message offset for each requsted topic/partition
        /// </summary>
        /// <param name="topics"></param>List of topics to get offset for
        /// <param name="time"></param>Used to ask for all messages before a certain time (ms). Specify -1 to receive the latest offsets and -2 to receive the earliest available offset. Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// <param name="maxNumOffsets"></param>Max number of offsets to receive. Returns 2 when time = -1 firsrt and current. Returns 1 when time = -2
        /// <param name="correlationId"></param>Id used by the client to identify this transaction. Returned in the response
        /// <param name="clientId"></param>Name to identify the client. Used in server logs
        /// <param name="partitionId"></param>Requested partition
        /// <returns></returns>
        public OffsetResponse GetOffsetResponse(List<string> topics, long? time, int maxNumOffsets, int correlationId, string clientId, int partitionId)
        {
            var requestTime = time.GetValueOrDefault(OffsetRequest.LatestTime);

            var request = new OffsetRequest(correlationId, clientId);
            foreach (var topicName in topics)
            {
                request.AddTopic(topicName, partitionId, requestTime, maxNumOffsets);
            }
            return GetOffsetResponseBefore(request);
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">The offset request.</param>
        /// <returns>List of offsets, in descending order.</returns>
        public OffsetResponse GetOffsetResponseBefore(OffsetRequest request)
        {
            using (var connection = new KafkaConnection(server, port))
            {
                connection.Write(request.GetRequestBytes().ToArray());

                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                if (dataLength == 0)
                    return null;
                byte[] data = connection.Read(dataLength);
                var offsetResponse = new OffsetResponse(data);
                return offsetResponse;
            }
        }

        /// <summary>
        /// Get meta data for a topic
        /// </summary>
        /// <param name="correlationId"></param>Id used by the client to identify this transaction. Returned in the response
        /// <param name="clientId"></param>Name to identify the client. Used in server logs
        /// <param name="topicName"></param> Name of the requested topic. If topic name is null metadata for all topics will be returned
        /// <returns></returns>
        public MetadataResponse Metadata(int correlationId, string clientId, String topicName)
        {
            MetadataRequest request = new MetadataRequest(correlationId, clientId, topicName);

            using (var connection = new KafkaConnection(server, port))
            {
                connection.Write(request.GetRequestBytes().ToArray());

                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                if (dataLength == 0)
                    return null;
                byte[] data = connection.Read(dataLength);
                MetadataResponse metadataResponse = new MetadataResponse();
                metadataResponse.Parse(data, 0);
                return metadataResponse;
            }
        }

        public ProduceResponse Produce(int correlationId, string clientId, int timeOut, string topicName, int partitionId, byte[] payLoad)
        {
            var request = new ProduceRequest(timeOut, correlationId, clientId);
            request.AddMessage(topicName, partitionId, payLoad);
            using (var connection = new KafkaConnection(server, port))
            {
                connection.Write(request.GetRequestBytes().ToArray());

                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                var response = new ProduceResponse();
                if (dataLength != 0)
                {
                    byte[] data = connection.Read(dataLength);
                    response.Parse(data);
                }
                return response;
            }
        }
    }
}
