using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitAggregateNode(AggregateNode node)
    {
        Console.WriteLine($"[WARN] Agregasi tidak diimplementasikan");

        return node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));
    }
}