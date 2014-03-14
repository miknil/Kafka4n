using System;

namespace KafkaBusClient.Exception
{
    public class KafkaException : ApplicationException
    {
        private readonly string message;
        private short errorCode;
        public KafkaException(string message)
            :base(message)
        {
            errorCode = 3;
        }

        public KafkaException(short errorCode)
        {
            this.errorCode = errorCode;
            switch (errorCode)
            {
                case 1:
                    message = "OffsetOutOfRange";
                    break;
                case 2:
                    message = "InvalidMessage";
                    break;
                case 3:
                    message = "UnknownTopicOrPartition";
                    break;
                case 4:
                    message = "InvalidMessageSize";
                    break;
                case 5:
                    message = "LeaderNotAvailable";
                    break;
                case 6:
                    message = "NotLeaderForPartition";
                    break;
                case 7:
                    message = "RequestTimedOut";
                    break;
                case 8:
                    message = "BrokerNotAvailable";
                    break;
                case 9:
                    message = "ReplicaNotAvailable";
                    break;
                case 10:
                    message = "MessageSizeTooLarge";
                    break;

                default:
                    message = "Unknown";
                    break;
            }
        }

        public override string Message
        {
            get { return message; }
        }

        public short ErrorCode
        {
            get { return errorCode; }
        }
    }
}
