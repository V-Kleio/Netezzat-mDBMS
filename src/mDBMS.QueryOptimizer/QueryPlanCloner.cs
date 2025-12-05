using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer;

internal static class QueryPlanCloner
{
    public static QueryPlan Clone(QueryPlan source)
    {
        return new QueryPlan
        {
            PlanId = source.PlanId,
            OriginalQuery = CloneQuery(source.OriginalQuery),
            PlanTree = source.PlanTree != null ? ClonePlanNode(source.PlanTree) : null,
            TotalEstimatedCost = source.TotalEstimatedCost,
            EstimatedRows = source.EstimatedRows,
            Strategy = source.Strategy,
            CreatedAt = source.CreatedAt
        };
    }

    private static Query CloneQuery(Query source)
    {
        return new Query
        {
            Table = source.Table,
            SelectedColumns = [.. source.SelectedColumns],
            WhereClause = source.WhereClause,
            Joins = source.Joins?.Select(CloneJoin).ToList(),
            OrderBy = source.OrderBy?.Select(CloneOrder).ToList(),
            GroupBy = source.GroupBy != null ? [.. source.GroupBy] : null,
            Type = source.Type
        };
    }

    private static JoinOperation CloneJoin(JoinOperation join)
    {
        return new JoinOperation
        {
            LeftTable = join.LeftTable,
            RightTable = join.RightTable,
            OnCondition = join.OnCondition,
            Type = join.Type
        };
    }

    private static OrderByOperation CloneOrder(OrderByOperation order)
    {
        return new OrderByOperation
        {
            Column = order.Column,
            IsAscending = order.IsAscending
        };
    }

    /// <summary>
    /// Deep clone a PlanNode tree recursively.
    /// </summary>
    private static PlanNode ClonePlanNode(PlanNode node)
    {
        return node switch
        {
            // Leaf nodes
            TableScanNode tsn => new TableScanNode
            {
                TableName = tsn.TableName,
                NodeCost = tsn.NodeCost
            },
            IndexScanNode isn => new IndexScanNode
            {
                TableName = isn.TableName,
                IndexColumn = isn.IndexColumn,
                NodeCost = isn.NodeCost
            },
            IndexSeekNode isk => new IndexSeekNode
            {
                TableName = isk.TableName,
                IndexColumn = isk.IndexColumn,
                SeekConditions = isk.SeekConditions.ToList(),
                NodeCost = isk.NodeCost
            },
            
            // Unary nodes
            FilterNode fn => new FilterNode(ClonePlanNode(fn.Input), fn.Conditions.ToList())
            {
                NodeCost = fn.NodeCost
            },
            ProjectNode pn => new ProjectNode(ClonePlanNode(pn.Input), pn.Columns.ToList())
            {
                NodeCost = pn.NodeCost
            },
            SortNode sn => new SortNode(ClonePlanNode(sn.Input), sn.OrderBy.Select(CloneOrder).ToList())
            {
                NodeCost = sn.NodeCost
            },
            AggregateNode an => new AggregateNode(ClonePlanNode(an.Input), an.GroupBy.ToList())
            {
                NodeCost = an.NodeCost
            },
            
            // Binary nodes
            JoinNode jn => new JoinNode(ClonePlanNode(jn.Left), ClonePlanNode(jn.Right), jn.JoinType, CloneCondition(jn.JoinCondition))
            {
                Algorithm = jn.Algorithm,
                NodeCost = jn.NodeCost
            },
            
            _ => throw new NotSupportedException($"Unknown PlanNode type: {node.GetType().Name}")
        };
    }

    private static Condition CloneCondition(Condition cond)
    {
        return new Condition
        {
            lhs = cond.lhs,
            rhs = cond.rhs,
            opr = cond.opr,
            rel = cond.rel
        };
    }
}