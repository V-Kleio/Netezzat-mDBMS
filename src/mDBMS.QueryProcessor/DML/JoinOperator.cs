using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitJoinNode(JoinNode node)
    {
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
}