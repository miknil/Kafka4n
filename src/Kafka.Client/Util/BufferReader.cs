using System;
using System.Linq;
using System.Text;

namespace Kafka.Client.Util
{
    public class BufferReader
    {
        public static int Read(byte[] data, int offset, out byte resultValue)
        {
            const int take = sizeof(byte);
            resultValue = data[offset];
            return offset + take;
        }
        public static int Read(byte[] data, int offset, out short resultValue)
        {
            const int take = sizeof(short);
            resultValue = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Skip(offset).Take(take).ToArray()), 0);
            return offset + take;
        }
        public static int Read(byte[] data, int offset, out int resultValue)
        {
            const int take = sizeof(int);
            resultValue = BitConverter.ToInt32(BitWorks.ReverseBytes(data.Skip(offset).Take(take).ToArray()), 0);
            return offset + take;
        }
        public static int Read(byte[] data, int offset, out long resultValue)
        {
            const int take = sizeof(long);
            resultValue = BitConverter.ToInt64(BitWorks.ReverseBytes(data.Skip(offset).Take(take).ToArray()), 0);
            return offset + take;
        }
        public static int Read(byte[] data, int offset, out String resultValue)
        {
            short topicNameLength;
            offset = Read(data, offset, out topicNameLength);

            byte[] tName = data.Skip(offset).Take(topicNameLength).ToArray();
            resultValue = Encoding.UTF8.GetString(tName);
            return offset + topicNameLength;
        }
    }
}
