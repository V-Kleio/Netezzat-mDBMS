using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitProjectNode(ProjectNode node)
    {
        foreach (Row row in node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)))
        {
            Row projectedRow = new() { id = row.id };

            foreach (string column in node.Columns)
            {
                projectedRow[column] = row[column];
            }

            yield return projectedRow;
        }
    }
}