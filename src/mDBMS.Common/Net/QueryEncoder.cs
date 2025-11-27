using System.Text;

namespace mDBMS.Common.Net
{
    public class QueryEncoder
    {
        private static readonly byte[] MagicBytes = Encoding.UTF8.GetBytes("mDBMS");

        public byte[] Encode(string query, int transactionId)
        {
            using (var memoryStream = new MemoryStream())
            {
                memoryStream.Write(MagicBytes, 0, MagicBytes.Length);

                var transactionIdBytes = BitConverter.GetBytes(transactionId);
                memoryStream.Write(transactionIdBytes, 0, transactionIdBytes.Length);

                var queryBytes = Encoding.UTF8.GetBytes(query);
                memoryStream.Write(queryBytes, 0, queryBytes.Length);

                return memoryStream.ToArray();
            }
        }
    }
}
