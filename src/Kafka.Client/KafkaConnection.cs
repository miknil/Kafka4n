using System;
using System.Net.Sockets;
using System.Threading;
using Kafka.Client.Request;

namespace Kafka.Client
{
    /// <summary>
    /// Manages connections to the Kafka.
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        /// <summary>
        /// TCP client that connects to the server.
        /// </summary>
        private readonly TcpClient client;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public KafkaConnection(string server, int port)
        {
            Server = server;
            Port = port;

            // connection opened
            client = new TcpClient(server, port);
        }

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Server { get; private set; }
        
        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Readds data from the server.
        /// </summary>
        /// <remarks>
        /// Defauls the amount of time that a read operation blocks waiting for data to <see cref="Timeout.Infinite"/>.
        /// </remarks>
        /// <param name="size">The number of bytes to read from the server.</param>
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read(int size)
        {
            return Read(size, Timeout.Infinite);
        }

        /// <summary>
        /// Readds data from the server.
        /// </summary>
        /// <param name="size">The number of bytes to read from the server.</param>
        /// <param name="readTimeout">The amount of time that a read operation blocks waiting for data.</param>
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read(int size, int readTimeout)
        {
            NetworkStream stream = client.GetStream();
            stream.ReadTimeout = readTimeout;

            byte[] bytes = new byte[size];
            bool readComplete = false;
            int numberOfTries = 0;

            while (!readComplete && numberOfTries < 1000)
            {
                if (stream.DataAvailable)
                {
                    stream.Read(bytes, 0, size);
                    readComplete = true;
                }
                else
                {
                    // wait until the server is ready to send some stuff.
                    numberOfTries++;
                    Thread.Sleep(10);
                }
            } 
            
            return bytes;
        }
        

        /// <summary>
        /// Writes a producer request to the server asynchronously.
        /// </summary>
        /// <remarks>
        /// The default callback simply calls the <see cref="NetworkStream.EndWrite"/>. This is
        /// basically a low level fire and forget call.
        /// </remarks>
        /// <param name="data">The data to send to the server.</param>
        public void BeginWrite(byte[] data)
        {
            NetworkStream stream = client.GetStream();
            stream.BeginWrite(data, 0, data.Length, asyncResult => ((NetworkStream)asyncResult.AsyncState).EndWrite(asyncResult), stream);
        }

        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infinite.
        /// </remarks>
        /// <param name="data">The data to write to the server.</param>
        public void Write(byte[] data)
        {
            Write(data, Timeout.Infinite);
        }

        /// <summary>
        /// Writes a producer request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="ProducerRequest"/> to send to the server.</param>
        public void Write(ProduceRequest request)
        {
            Write(request.GetBytes());
        }


        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <param name="data">The data to write to the server.</param>
        /// <param name="writeTimeout">The amount of time that a write operation blocks waiting for data.</param>
        public void Write(byte[] data, int writeTimeout)
        {
            NetworkStream stream = client.GetStream();
            stream.WriteTimeout = writeTimeout;

            // Send the message to the connected TcpServer. 
            stream.Write(data, 0, data.Length);
        }

        /// <summary>
        /// Close the connection to the server.
        /// </summary>
        public void Dispose()
        {
            if (client != null)
            {
                client.GetStream().Close();
                client.Close();
            }
        }
    }
}
