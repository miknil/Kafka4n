using System;
using Kafka.Client.Util;

namespace Kafka.Client.Response
{
    public class Broker
    {

        //Broker => NodeId Host Port
        //NodeId => int32
        //Host => string
        //Port => int32

        private int nodeId;
        private string host;
        private int port;

        public int  NodeId
        {
            get { return nodeId; }
            set { nodeId = value; }
        }

        public String Host
        {
            get { return host; }
            set { host = value; }
        }

        public int Port
        {
            get { return port; }
            set { port = value; }
        }

        public int Parse(byte[] data, int dataOffset)
        {
            int bufferOffset = dataOffset;
            bufferOffset = BufferReader.Read(data, bufferOffset, out nodeId);
            bufferOffset = BufferReader.Read(data, bufferOffset, out host);
            bufferOffset = BufferReader.Read(data, bufferOffset, out port);
            return bufferOffset;
        }

        public override string ToString()
        {
            return host + ":" + port;
        }
    }
}
