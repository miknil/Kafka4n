using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    public class Topic : IRequestBuffer
    {
        private readonly String topicName;
        private List<IRequestBuffer> partitions = new List<IRequestBuffer>();

        public List<IRequestBuffer> Partitions
        {
            get { return partitions; }
            set { partitions = value; }
        }

        public Topic(String name)
        {
            topicName = name;
        }

        /// <summary>
        /// Serialize the topic and all contained partitions to a byte buffer
        /// </summary>
        /// <returns>Byte buffer containing the topic and partition data</returns>
        public List<byte> GetRequestBytes()
        {
            var request = new List<byte>();
            if (topicName == null)
                return request;
            request.AddRange(BitWorks.GetBytesReversed((short)topicName.Length));
            request.AddRange(Encoding.ASCII.GetBytes(topicName));
            if (Partitions.Count > 0)
            {
                request.AddRange(BitWorks.GetBytesReversed(Partitions.Count));
                foreach (IRequestBuffer partition in Partitions)
                {
                    request.AddRange(partition.GetRequestBytes());
                }
            }
            return request;
        }
    }
}
