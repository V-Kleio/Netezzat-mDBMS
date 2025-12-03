using System.Text.Json.Serialization;

namespace mDBMS.Common.Net;

class QueryPayload
{
    [JsonInclude] public int TransactionId;
    [JsonInclude] public string Query = "";

    public QueryPayload(int transactionId, string query)
    {
        this.TransactionId = transactionId;
        this.Query = query;
    }
}