using System.Collections.Generic;

namespace Kafka.Client.Request
{
    public interface IRequestBuffer
    {
        List<byte> GetRequestBytes();
    }
}
