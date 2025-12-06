using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitJoinNode(JoinNode node)
    {
        if (node.JoinCondition == null || node.JoinType == JoinType.CROSS)
        {
            return CrossJoin(node);
        }

        if (node.JoinCondition.lhs is not string)
        {
            throw new Exception("cannot parse left hand side of join condition");
        }

        if (node.JoinCondition.rhs is not string)
        {
            throw new Exception("cannot parse right hand side of join condition");
        }

        return node.Algorithm switch
        {
            JoinAlgorithm.NestedLoop => NestedLoopJoin(node),
            JoinAlgorithm.Merge => MergeJoin(node),
            JoinAlgorithm.Hash => HashJoin(node),
            _ => throw new Exception("unknown join algorithm"),
        };
    }

    private IEnumerable<Row> CrossJoin(JoinNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Cross Join (Cartesian Product) antara 2 tabel");

        IEnumerable<Row> lhs = node.Left.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));
        IEnumerable<Row> rhs = node.Right.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));

        List<Row> rightRows = rhs.ToList();

        foreach (Row leftRow in lhs)
        {
            foreach (Row rightRow in rightRows)
            {
                Row row = new();

                foreach (var (key, val) in leftRow.Columns)
                {
                    row[key] = val;
                }

                foreach (var (key, val) in rightRow.Columns)
                {
                    row[key] = val;
                }

                row.id = (leftRow.id ?? "") + ";" + (rightRow.id ?? "");

                yield return row;
            }
        }
    }
}
