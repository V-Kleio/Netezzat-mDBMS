using System.Runtime.CompilerServices;
using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> NestedLoopJoin(JoinNode node)
    {
        IEnumerable<Row> lhs = node.Left.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));
        IEnumerable<Row> rhs = node.Right.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));

        List<Row> remainder = [];

        Dictionary<string, Type>? leftKeys = null;
        Dictionary<string, Type>? rightKeys = null;

        switch (node.JoinType)
        {
            case JoinType.RIGHT:
                (lhs, rhs) = (rhs, lhs);
                break;
            case JoinType.FULL:
                remainder = rhs.ToList();
                break;
        }

        foreach (Row leftRow in lhs)
        {
            bool matches = false;

            if (leftKeys is null)
            {
                leftKeys = [];

                foreach (var (key, val) in leftRow.Columns)
                {
                    leftKeys[key] = val.GetType();
                }
            }

            foreach (Row rightRow in rhs)
            {
                if (rightKeys is null)
                {
                    rightKeys = [];
    
                    foreach (var (key, val) in rightRow.Columns)
                    {
                        rightKeys[key] = val.GetType();
                    }
                }

                if (!leftRow.Columns.ContainsKey((string) node.JoinCondition.lhs))
                {
                    throw new Exception("join column not found on lhs");
                }

                if (!rightRow.Columns.ContainsKey((string) node.JoinCondition.rhs))
                {
                    throw new Exception("join column not found on rhs");
                }

                if (leftRow[(string) node.JoinCondition.lhs] == rightRow[(string) node.JoinCondition.rhs])
                {
                    matches = true;

                    if (node.JoinType == JoinType.FULL)
                    {
                        int idx = remainder.FindIndex(row => row.id == rightRow.id);
                        if (idx >= 0)
                        {
                            remainder.RemoveAt(idx);
                        }
                    }

                    Row row = new();
                    
                    foreach (var (key, val) in leftRow.Columns)
                    {
                        row[key] = val;
                    }

                    foreach (var (key, val) in rightRow.Columns)
                    {
                        row[key] = val;
                    }

                    row.id = leftRow.id + ";" + rightRow.id;

                    yield return row;
                }
            }

            if (!matches && node.JoinType != JoinType.INNER)
            {
                Row row = new() { id = leftRow.id };

                foreach (var (key, val) in leftRow.Columns)
                {
                    row[key] = val;
                }

                foreach (var (key, val) in rightKeys!)
                {
                    row[key] = RuntimeHelpers.GetUninitializedObject(val);
                }

                yield return row;
            }
        }

        if (remainder.Count > 0)
        {
            foreach (Row rem in remainder)
            {
                Row row = new() { id = rem.id };

                foreach (var (key, val) in rem.Columns)
                {
                    row[key] = val;
                }

                foreach (var (key, val) in rightKeys!)
                {
                    row[key] = RuntimeHelpers.GetUninitializedObject(val);
                }

                yield return row;
            }
        }
    }
}