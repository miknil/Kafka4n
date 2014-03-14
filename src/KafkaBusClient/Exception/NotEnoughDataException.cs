using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaBusClient.Exception
{
    public class NotEnoughDataException : KafkaException
    {
        public NotEnoughDataException(string message) : base(message)
        {
        }
    }
}
