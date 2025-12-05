using System.Text.Json;

namespace mDBMS.Common.Net
{
    public class QueryDecoder
    {
        public (string query, int transactionId) Decode(byte[] data, int lowerbound, int upperbound)
        {
            int length = upperbound - lowerbound;

            var span = new Span<byte>(data, lowerbound, length);

            QueryPayload? payload = JsonSerializer.Deserialize<QueryPayload>(span.ToString());

            if (payload is null)
            {
                throw new Exception("could not deserialize query payload");
            }

            return (payload.Query, payload.TransactionId);
        }
    }
}
