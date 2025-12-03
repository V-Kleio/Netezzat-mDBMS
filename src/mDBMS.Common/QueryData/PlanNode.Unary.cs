using mDBMS.Common.Data;

namespace mDBMS.Common.QueryData;

/// <summary>
/// Unary Node: Filter - menerapkan kondisi WHERE pada hasil input.
/// Memfilter baris yang tidak memenuhi kondisi.
/// </summary>
public sealed class FilterNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Kondisi boolean yang harus dipenuhi.
    /// Baris yang menghasilkan false akan dibuang.
    /// </summary>
    public IEnumerable<Condition> Conditions { get; set; }

    /// <summary>
    /// Total cost = node cost + input cost.
    /// </summary>
    public override double TotalCost => NodeCost + Input.TotalCost;

    public override string OperationName => "FILTER";
    public override string Details => $"WHERE {Conditions}";

    public FilterNode(PlanNode input, IEnumerable<Condition> conditions)
    {
        Input = input;
        Conditions = conditions;
    }

    public override List<QueryPlanStep> ToSteps()
    {
        var steps = Input.ToSteps();
        steps.Add(new QueryPlanStep
        {
            Order = steps.Count + 1,
            Operation = OperationType.FILTER,
            Description = Details,
            EstimatedCost = NodeCost
        });
        return steps;
    }
}

/// <summary>
/// Unary Node: Projection - memilih kolom-kolom tertentu dari input.
/// Mengurangi lebar baris (jumlah kolom).
/// </summary>
public sealed class ProjectNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Daftar kolom yang akan di-output.
    /// Kolom lain akan dibuang.
    /// </summary>
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// Total cost = node cost + input cost.
    /// </summary>
    public override double TotalCost => NodeCost + Input.TotalCost;

    public override string OperationName => "PROJECT";
    public override string Details => $"Columns: {string.Join(", ", Columns)}";

    public ProjectNode(PlanNode input, List<string> columns)
    {
        Input = input;
        Columns = columns;
    }

    public override List<QueryPlanStep> ToSteps()
    {
        var steps = Input.ToSteps();
        steps.Add(new QueryPlanStep
        {
            Order = steps.Count + 1,
            Operation = OperationType.PROJECTION,
            Description = Details,
            EstimatedCost = NodeCost
        });
        return steps;
    }
}

/// <summary>
/// Unary Node: Sort - mengurutkan hasil berdasarkan kolom tertentu.
/// Operasi mahal (O(n log n)), sebaiknya dilakukan di akhir pipeline.
/// </summary>
public sealed class SortNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Daftar kolom untuk sorting beserta arah (ASC/DESC).
    /// </summary>
    public List<OrderByOperation> OrderBy { get; set; } = new();

    /// <summary>
    /// Total cost = node cost + input cost.
    /// </summary>
    public override double TotalCost => NodeCost + Input.TotalCost;

    public override string OperationName => "SORT";
    public override string Details => $"ORDER BY {string.Join(", ", OrderBy.Select(o => $"{o.Column} {(o.IsAscending ? "ASC" : "DESC")}"))}";

    public SortNode(PlanNode input, List<OrderByOperation> orderBy)
    {
        Input = input;
        OrderBy = orderBy;
    }

    public override List<QueryPlanStep> ToSteps()
    {
        var steps = Input.ToSteps();
        steps.Add(new QueryPlanStep
        {
            Order = steps.Count + 1,
            Operation = OperationType.SORT,
            Description = Details,
            EstimatedCost = NodeCost
        });
        return steps;
    }
}

/// <summary>
/// Unary Node: Aggregation - melakukan grouping dan aggregate functions.
/// </summary>
public sealed class AggregateNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Kolom untuk GROUP BY.
    /// </summary>
    public List<string> GroupBy { get; set; } = new();

    /// <summary>
    /// Total cost = node cost + input cost.
    /// </summary>
    public override double TotalCost => NodeCost + Input.TotalCost;

    public override string OperationName => "AGGREGATE";
    public override string Details => $"GROUP BY {string.Join(", ", GroupBy)}";

    public AggregateNode(PlanNode input, List<string> groupBy)
    {
        Input = input;
        GroupBy = groupBy;
    }

    public override List<QueryPlanStep> ToSteps()
    {
        var steps = Input.ToSteps();
        steps.Add(new QueryPlanStep
        {
            Order = steps.Count + 1,
            Operation = OperationType.AGGREGATION,
            Description = Details,
            EstimatedCost = NodeCost
        });
        return steps;
    }
}
