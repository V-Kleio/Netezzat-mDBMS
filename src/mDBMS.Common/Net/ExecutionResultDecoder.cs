using System.Text.Json;
using mDBMS.Common.Transaction;

namespace mDBMS.Common.Net;

public class ExecutionResultDecoder
{
    public ExecutionResult Decode(byte[] data, int lowerbound, int upperbound)
    {
        int length = upperbound - lowerbound;

        var span = new Span<byte>(data, lowerbound, length);

        ExecutionResultPayload? payload = JsonSerializer.Deserialize<ExecutionResultPayload>(span.ToString());

        if (payload is null)
        {
            throw new Exception("could not deserialize execution result payload");
        }

        return payload.Extract();
    }
}
