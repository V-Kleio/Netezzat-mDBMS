using mDBMS.Common.Data;
using mDBMS.Common.Transaction;
using System.Text;

namespace mDBMS.Common.Net;

public class ExecutionResultDecoder
{
    public ExecutionResult Decode(byte[] data, int lowerbound, int upperbound)
    {
        var result = new ExecutionResult();
        using (var memoryStream = new MemoryStream(data, lowerbound, upperbound - lowerbound, false))
        using (var reader = new BinaryReader(memoryStream, Encoding.UTF8))
        {
            result.Success = reader.ReadBoolean();
            result.Message = reader.ReadString();

            if (reader.ReadBoolean())
            {
                result.TransactionId = reader.ReadInt32();
            }

            int rowCount = reader.ReadInt32();
            if (rowCount > 0)
            {
                var rows = new List<Row>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    int colCount = reader.ReadInt32();
                    var rowData = new Dictionary<string, object>(colCount);
                    for (int j = 0; j < colCount; j++)
                    {
                        var key = reader.ReadString();
                        var typeName = reader.ReadString();
                        var valueStr = reader.ReadString();

                        if (typeName == "null")
                        {
                            rowData[key] = null!;
                        }
                        else
                        {
                            var type = Type.GetType(typeName);
                            if (type != null)
                            {
                                rowData[key] = Convert.ChangeType(valueStr, type);
                            }
                            else
                            {
                                rowData[key] = valueStr;
                            }
                        }
                    }
                    var row = new Row();
                    row.Columns = rowData;
                    rows.Add(row);
                }
                result.Data = rows;
            }
        }
        return result;
    }
}