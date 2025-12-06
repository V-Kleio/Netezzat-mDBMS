using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitIndexScanNode(IndexScanNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Index Scan untuk tabel: {node.TableName}");

        foreach (Row row in storageManager.ReadBlock(new(node.TableName, [])))
        {
            Row canonRow = new() { id = row.id };

            foreach (var (key, val) in row.Columns)
            {
                canonRow[$"{node.TableName}." + key] = val;
            }

            Console.Write($"[INFO] Retrieved row with id: {row.id}");

            yield return canonRow;
        }
    }
}