using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitFilterNode(FilterNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Filter");

        foreach (var cond in node.Conditions)
        {
            Console.WriteLine($"       {cond.lhs} = {cond.rhs}");
        }

        foreach (Row row in node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)))
        {
            bool matches = true;

            foreach (Condition condition in node.Conditions)
            {
                object lhs;
                object rhs;

                switch (condition.rel)
                {
                    case Condition.Relation.COLUMN_AND_VALUE:
                        lhs = row[(string) condition.lhs];
                        rhs = condition.rhs;
                    break;
                    case Condition.Relation.VALUE_AND_COLUMN:
                        lhs = condition.lhs;
                        rhs = row[(string) condition.rhs];
                    break;
                    case Condition.Relation.COLUMN_AND_COLUMN:
                        lhs = row[(string) condition.lhs];
                        rhs = row[(string) condition.rhs];
                    break;
                    default:
                        throw new Exception("unknown condition relation");
                }

                bool valid = condition.opr switch
                {
                    Condition.Operation.EQ => Equals(lhs, rhs),
                    Condition.Operation.NEQ => !Equals(lhs, rhs),
                    Condition.Operation.GT => ((IComparable) lhs).CompareTo(rhs) > 0,
                    Condition.Operation.LT => ((IComparable) lhs).CompareTo(rhs) < 0,
                    Condition.Operation.GEQ => ((IComparable) lhs).CompareTo(rhs) >= 0,
                    Condition.Operation.LEQ => ((IComparable) lhs).CompareTo(rhs) <= 0,
                    _ => throw new Exception("unknown condition operator")
                };

                if (!valid)
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                yield return row;
            }
        }
    }
}
