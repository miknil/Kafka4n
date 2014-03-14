using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaBusClient.Exception
{
    public enum KafkaErrorCode
    {
        /// <summary> No error--it worked! </summary>
        NoError = 0,

        /// <summary> An unexpected server error </summary>
        Unknown = -1,

        /// <summary> The requested offset is outside the range of offsets maintained by the server for the given topic/partition. </summary>
        OffsetOutOfRange = 1,

        /// <summary> This indicates that a message contents does not match its CRC </summary>
        InvalidMessage = 2,
        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The message has a negative size
        /// </summary>
        InvalidMessageSize = 4,
        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5,
        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,
        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        RequestTimedOut = 7,
        /// <summary>
        /// This is not a client facing error and is used only internally by intra-cluster broker communication.
        /// </summary>
        BrokerNotAvailable = 8,
        /// <summary>
        /// What is the difference between this and LeaderNotAvailable?
        /// </summary>
        ReplicaNotAvailable = 9,
        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10, 
        /// <summary>
        /// ???
        /// </summary>
        StaleControllerEpochCode = 11, 
        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata
        /// </summary>
        OffsetMetadataTooLargeCode = 12, 
    }
}
