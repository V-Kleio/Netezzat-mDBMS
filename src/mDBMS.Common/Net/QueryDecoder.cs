using System;
using System.Text;

namespace mDBMS.Common.Net
{
    public class QueryDecoder
    {
        private static readonly byte[] MagicBytes = Encoding.UTF8.GetBytes("mDBMS");
        private static readonly int MetadataSize = MagicBytes.Length + sizeof(int);

        public (string query, int transactionId) Decode(byte[] data, int length)
        {
            if (length < MetadataSize)
            {
                throw new ArgumentException("Data is too short to be a valid query.");
            }

            var span = new Span<byte>(data, 0, length);

            if (!span.StartsWith(MagicBytes))
            {
                throw new ArgumentException("Invalid magic bytes. This is not a valid mDBMS query.");
            }

            var transactionId = BitConverter.ToInt32(span.Slice(MagicBytes.Length, sizeof(int)));
            var query = Encoding.UTF8.GetString(span.Slice(MetadataSize));

            return (query, transactionId);
        }
    }
}
