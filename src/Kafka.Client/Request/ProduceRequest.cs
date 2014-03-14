using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProduceRequest : IRequestBuffer
    {
        public const short NoAck = 0;
        public const short AckOne = 1;
        public const short AckAll = -1;
        private short requiredAcks;
        private int timeOut;

        private List<ProduceTopic>  produceTopics = new List<ProduceTopic>();
        private Request request;

        public ProduceRequest(int timeOut, int correlationId, string clientId)
            : this(AckOne, timeOut, correlationId, clientId)
        {
        }

        public ProduceRequest(short requiredAcks, int timeOut, int correlationId, string clientId)
        {
            request = new Request(Request.Produce, correlationId, clientId);

            this.requiredAcks = requiredAcks;
            this.timeOut = timeOut;
        }

        public ProduceTopic AddTopic(string topicName)
        {
            ProduceTopic topic = new ProduceTopic(topicName);
            produceTopics.Add(topic);
            return topic;
        }


        public MessageSet AddMessage(string topicName, int partitionId, byte[] payload)
        {
            var topic = AddTopic(topicName);
            var partition = topic.AddPartition(partitionId);
            var messageSet = new MessageSet(payload);
            partition.AddMessageSet(messageSet);
            return messageSet;
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        public ProduceRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public ProduceRequest(string topic, int partition, IList<Message> messages)
        {
            //Topic = topic;
            //Partition = partition;
            //Messages = messages;
        }

        /// <summary>
        /// Gets or sets the messages to publish.
        /// </summary>
        public IList<Message> Messages { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        //public bool IsValid()
        //{
        //    return !String.IsNullOrEmpty(Topic) && Messages != null && Messages.Count > 0;
        //}

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public  byte[] GetBytes()
        {
            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(GetInternalBytes());

            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Produce));
            encodedMessageSet.InsertRange(0, requestBytes);
            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }

        /// <summary>
        /// Gets the bytes representing the request which is used when generating a multi-request.
        /// </summary>
        /// <remarks>
        /// The <see cref="GetBytes"/> method is used for sending a single <see cref="RequestType.Produce"/>.
        /// It prefixes this byte array with the request type and the number of messages. This method
        /// is used to supply the <see cref="MultiProducerRequest"/> with the contents for its message.
        /// </remarks>
        /// <returns>The bytes that represent this <see cref="ProducerRequest"/>.</returns>
        internal byte[] GetInternalBytes()
        {
            List<byte> messagePack = new List<byte>();
            foreach (Message message in Messages)
            {
                byte[] messageBytes = message.GetBytes();
                messagePack.AddRange(BitWorks.GetBytesReversed(messageBytes.Length));
                messagePack.AddRange(messageBytes);
            }

            //byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(Topic.Length));
            //byte[] topicBytes = Encoding.UTF8.GetBytes(Topic);
            //byte[] partitionBytes = BitWorks.GetBytesReversed(Partition);
            byte[] messagePackLengthBytes = BitWorks.GetBytesReversed(messagePack.Count);
            byte[] messagePackBytes = messagePack.ToArray();

            List<byte> encodedMessageSet = new List<byte>();
            //encodedMessageSet.AddRange(topicLengthBytes);
            //encodedMessageSet.AddRange(topicBytes);
            //encodedMessageSet.AddRange(partitionBytes);
            //encodedMessageSet.AddRange(messagePackLengthBytes);
            //encodedMessageSet.AddRange(messagePackBytes);

            return encodedMessageSet.ToArray();
        }

        public List<byte> GetRequestBytes()
        {
            var requestBytes = new List<byte>();
            // Get request base: ApiKey, ApiVersion, CorrelationId, ClientId
            requestBytes.AddRange(request.GetRequestBytes());

            requestBytes.AddRange(BitWorks.GetBytesReversed(requiredAcks));
            requestBytes.AddRange(BitWorks.GetBytesReversed(timeOut));
            requestBytes.AddRange(BitWorks.GetBytesReversed(produceTopics.Count));
            foreach (var produceTopic in produceTopics)
            {
                requestBytes.AddRange(produceTopic.GetRequestBytes());
            }
            requestBytes.InsertRange(0, BitWorks.GetBytesReversed(Convert.ToInt32(requestBytes.Count)));
            
            return requestBytes;
        }
    }
}
