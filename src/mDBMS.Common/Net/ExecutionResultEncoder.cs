using mDBMS.Common.Transaction;
using System.Text;
using System.Text.Json;

namespace mDBMS.Common.Net
{
    public class ExecutionResultEncoder
    {
        public byte[] Encode(ExecutionResult result)
        {
            var payload = new ExecutionResultPayload(result);
            string data = JsonSerializer.Serialize(payload);

            return Encoding.UTF8.GetBytes(data);
        }
    }
}
