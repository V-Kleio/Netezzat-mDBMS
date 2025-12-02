using mDBMS.Common.Transaction;
using System.Text;

namespace mDBMS.Common.Net
{
    public class ExecutionResultEncoder
    {
        public byte[] Encode(ExecutionResult result)
        {
            using (var memoryStream = new MemoryStream())
            using (var writer = new BinaryWriter(memoryStream, Encoding.UTF8))
            {
                writer.Write(result.Success);
                writer.Write(result.Message ?? string.Empty);

                // temp commented soalnya executionresult gada transac id
                // writer.Write(result.TransactionId.HasValue);
                // if (result.TransactionId.HasValue)
                // {
                //     writer.Write(result.TransactionId.Value);
                // }

                if (result.Data != null)
                {
                    var rows = result.Data.ToList();
                    writer.Write(rows.Count);

                    foreach (var row in rows)
                    {
                        writer.Write(row.Columns.Count);
                        foreach (var pair in row.Columns)
                        {
                            writer.Write(pair.Key);

                            if (pair.Value == null)
                            {
                                writer.Write("null");
                            }
                            else
                            {
                                writer.Write(pair.Value.GetType().ToString());
                                writer.Write(pair.Value.ToString() ?? string.Empty);
                            }
                        }
                    }
                }
                else
                {
                    writer.Write(0); // No rows
                }

                return memoryStream.ToArray();
            }
        }
    }
}
