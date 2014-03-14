using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{

    /**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K. Key length is -1 if no key used
 * 5. K byte key if key used
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
*/
    /// <summary>
    /// Message for Kafka.
    /// </summary>
    /// <remarks>
    /// A message. The format of an N byte message is the following:
    /// <list type="bullet">
    ///     <item>
    ///         <description>1 byte "magic" identifier to allow format changes</description>
    ///     </item>
    ///     <item>
    ///         <description>4 byte CRC32 of the payload</description>
    ///     </item>
    ///     <item>
    ///         <description>N - 5 byte payload</description>
    ///     </item>
    /// </list>
    /// </remarks>
    public class Message
    {
        /// <summary>
        /// Magic identifier for Kafka.
        /// </summary>
        private static readonly byte DefaultMagicIdentifier = 0;

        private byte attributes = 0;
        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Uses the <see cref="DefaultMagicIdentifier"/> as a default.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        public Message(byte[] payload) : this(payload, DefaultMagicIdentifier)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Initializes the checksum as null.  It will be automatically computed.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        public Message(byte[] payload, byte magic) : this(payload, magic, null, null)
        {
        }

        public byte[] Key { get; set; }
        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        /// <param name="checksum">The checksum for the payload.</param>
        public Message(byte[] payload, byte magic, byte[] checksum, byte[] key)
        {
            Payload = payload;
            Magic = magic;
            Key = key;
            Checksum = checksum == null ? CalculateChecksum() : checksum;
        }
    
        /// <summary>
        /// Gets the magic bytes.
        /// </summary>
        public byte Magic { get; private set; }
        
        /// <summary>
        /// Gets the CRC32 checksum for the payload.
        /// </summary>
        public byte[] Checksum { get; private set; }

        /// <summary>
        /// Gets the payload.
        /// </summary>
        public byte[] Payload { get; private set; }

        /// <summary>
        /// Parses a message from a byte array given the format Kafka likes. 
        /// </summary>
        /// <param name="data">The data for a message.</param>
        /// <returns>The message.</returns>
        public static Message ParseFrom(byte[] data)
        {
            int dataOffset = 0;
            const int checksumLength = 4;
            byte[] checksum = data.Skip(dataOffset).Take(checksumLength).ToArray();
            dataOffset += checksumLength;
            byte magic;
            dataOffset = BufferReader.Read(data, dataOffset, out magic);
            byte attribute;
            dataOffset = BufferReader.Read(data, dataOffset, out attribute);
            int keyLength;
            dataOffset = BufferReader.Read(data, dataOffset, out keyLength);
            byte[] key = null;
            if (keyLength > 0)
            {
                key = data.Skip(dataOffset).Take(keyLength).ToArray();
                dataOffset += keyLength;
            }
            int payloadLength;
            dataOffset = BufferReader.Read(data, dataOffset, out payloadLength);
            byte[] payload = data.Skip(dataOffset).Take(payloadLength).ToArray();
            if (payload.Length < payloadLength)
                throw new IndexOutOfRangeException("Premature end of data. Payload length:" + payloadLength + " Actual:" + payload.Length);

            return new Message(payload, magic, checksum, key);
        }

        /// <summary>
        /// Converts the message to bytes in the format Kafka likes.
        /// </summary>
        /// <returns>The byte array.</returns>
        public byte[] GetBytes()
        {
            var buffer = new List<byte>();
            Checksum = CalculateChecksum();
            buffer.AddRange(Checksum);
            buffer.Add(Magic);
            buffer.Add(attributes);
            if (Key == null)
                buffer.AddRange(BitWorks.GetBytesReversed(-1));
            else
            {
                buffer.AddRange(BitWorks.GetBytesReversed(Key.Count()));
                buffer.AddRange(Key);
            }
            buffer.AddRange(BitWorks.GetBytesReversed(Payload.Count()));
            buffer.AddRange(Payload);

            return buffer.ToArray();
        }

        /// <summary>
        /// Converts the message to bytes in the format Kafka likes.
        /// </summary>
        /// <returns>The byte array.</returns>
        private byte[] GetMessageBytesWithoutCrc()
        {
            var buffer = new List<byte>();
            buffer.Add(Magic);
            buffer.Add(attributes);
            if (Key == null)
                buffer.AddRange(BitWorks.GetBytesReversed(-1));
            else
            {
                buffer.AddRange(BitWorks.GetBytesReversed(Key.Count()));
                buffer.AddRange(Key);
            }
            buffer.AddRange(BitWorks.GetBytesReversed(Payload.Count()));
            buffer.AddRange(Payload);

            return buffer.ToArray();
        }

        /// <summary>
        /// Try to show the payload as decoded to UTF-8.
        /// </summary>
        /// <returns>The decoded payload as string.</returns>
        public override string ToString()
        {
            return Encoding.UTF8.GetString(Payload);
        }

        /// <summary>
        /// Calculates the CRC32 checksum on the payload of the message.
        /// </summary>
        /// <returns>The checksum given the payload.</returns>
        private byte[] CalculateChecksum()
        { 
            Crc32 crc32 = new Crc32();
            return crc32.ComputeHash(GetMessageBytesWithoutCrc());
        }
    }
}
