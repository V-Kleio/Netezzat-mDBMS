using System.Text;
using System.Text.Json;

namespace mDBMS.Common.Net
{
    public class QueryEncoder
    {
        public static byte[] Encode(string query, int transactionId)
        {
            var payload = new QueryPayload(transactionId, query);
            string data = JsonSerializer.Serialize(payload);

            return Encoding.UTF8.GetBytes(data);
        }
    }
}
