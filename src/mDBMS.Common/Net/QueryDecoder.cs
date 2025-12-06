using System.Text;
using System.Text.Json;

namespace mDBMS.Common.Net
{
    public class QueryDecoder
    {
        public static (string query, int transactionId) Decode(byte[] data, int lowerbound, int upperbound)
        {
            int length = upperbound - lowerbound;

            var span = new Span<byte>(data, lowerbound, length);

            QueryPayload? payload = JsonSerializer.Deserialize<QueryPayload>(Encoding.UTF8.GetString(span)) ?? throw new Exception("could not deserialize query payload");
            return (payload.Query, payload.TransactionId);
        }
    }
}
