using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitProjectNode(ProjectNode node)
    {
        foreach (Row row in node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)))
        {
            Console.WriteLine($"[INFO] Melakukan Projection");

            Row projectedRow = new() { id = row.id };

            foreach (string column in node.Columns)
            {
                if (row.Columns.TryGetValue(column, out var value))
                {
                    projectedRow[column] = value;
                }
                else
                {
                    throw new Exception($"Column '{column}' not found in row. Available columns: {string.Join(", ", row.Columns.Keys)}");
                }
            }

            yield return projectedRow;
        }
    }
}
