using System.Text.Json.Serialization;

namespace mDBMS.Common.Net;

class QueryPayload(int transactionId, string query)
{
    [JsonInclude] public int TransactionId = transactionId;
    [JsonInclude] public string Query = query;
}
