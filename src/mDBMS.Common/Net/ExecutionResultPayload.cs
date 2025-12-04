using System.Text.Json.Serialization;
using mDBMS.Common.Data;
using mDBMS.Common.Transaction;

namespace mDBMS.Common.Net;

class ExecutionResultPayload
{
    public int TransactionId;
    public string Query;
    public bool Success;
    public string Message;
    public DateTime ExecutedAt;
    [JsonInclude] private IEnumerable<EncodedRow>? data;

    public ExecutionResultPayload(ExecutionResult result)
    {
        this.TransactionId = result.TransactionId;
        this.Query = result.Query;
        this.Success = result.Success;
        this.Message = result.Message;
        this.ExecutedAt = result.ExecutedAt;
        this.data = result.Data?.Select(row => new EncodedRow(row));
    }

    public ExecutionResult Extract()
    {
        return new()
        {
            TransactionId = this.TransactionId,
            Query = this.Query,
            Success = this.Success,
            Message = this.Message,
            ExecutedAt = this.ExecutedAt,
            Data = this.data?.Select(row => row.Extract())
        };
    }

    private class EncodedRow
    {
        public string Id;
        public Dictionary<string, EncodedDatum> Columns;

        public EncodedRow(Row row)
        {
            this.Id = row.id;
            this.Columns = [];

            foreach (var (key, val) in row.Columns)
            {
                this.Columns[key] = new EncodedDatum(val);
            }
        }

        public Row Extract()
        {
            Row row = new()
            {
                id = this.Id
            };

            foreach (var (key, val) in this.Columns)
            {
                row[key] = val.Extract()!;
            }

            return row;
        }

        public class EncodedDatum
        {
            public string type = typeof(int).ToString();
            public string value = "0";

            public EncodedDatum(object? datum)
            {
                string? value = datum?.ToString();

                if (value is null)
                {
                    this.type = "null";
                    this.value = "null";
                }
                else
                {
                    this.type = datum!.GetType().ToString();
                    this.value = value;
                }
            }

            public object? Extract()
            {
                if (this.type == "null" && this.value == "null")
                {
                    return null;
                }
                else if (this.type == "null" && this.value != "null")
                {
                    throw new Exception("inconsistent typing, null type has non-null value (suspected corruption)");
                }
                else
                {
                    Type? datumType = Type.GetType(this.type);

                    if (datumType is null)
                    {
                        throw new Exception("could not retrieve type of datum");
                    }

                    if (!datumType.GetInterfaces().Any(t => t.GetGenericTypeDefinition() == typeof(IParsable<>)))
                    {
                        throw new Exception("could not cast datum to its original type");
                    }

                    return datumType.GetMethod("Parse")!.Invoke(null, [this.value]);
                }
            }
        }
    }
}