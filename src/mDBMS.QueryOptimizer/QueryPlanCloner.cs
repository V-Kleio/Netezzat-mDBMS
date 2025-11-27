using mDBMS.Common.QueryData;

internal static class QueryPlanCloner
{
    public static QueryPlan Clone(QueryPlan source)
    {
        return new QueryPlan
        {
            PlanId = source.PlanId,
            OriginalQuery = CloneQuery(source.OriginalQuery),
            Steps = [.. source.Steps.Select(CloneStep)],
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

    private static QueryPlanStep CloneStep(QueryPlanStep step)
    {
        return new QueryPlanStep
        {
            Order = step.Order,
            Operation = step.Operation,
            Description = step.Description,
            Table = step.Table,
            IndexUsed = step.IndexUsed,
            EstimatedCost = step.EstimatedCost,
            Parameters = step.Parameters != null
                ? new Dictionary<string, object?>(step.Parameters, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        };
    }
}