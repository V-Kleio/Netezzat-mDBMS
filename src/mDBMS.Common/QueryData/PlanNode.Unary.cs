using mDBMS.Common.Data;

namespace mDBMS.Common.QueryData;

/// <summary>
/// Unary Node: Filter - menerapkan kondisi WHERE pada hasil input.
/// </summary>
public sealed class FilterNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Kondisi boolean yang harus dipenuhi.
    /// </summary>
    public IEnumerable<Condition> Conditions { get; set; }

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "FILTER";
    public override string Details => $"WHERE {string.Join(" AND ", Conditions)}";

    public FilterNode(PlanNode input, IEnumerable<Condition> conditions)
    {
        Input = input;
        Conditions = conditions;
    }
}

/// <summary>
/// Unary Node: Projection - memilih kolom-kolom tertentu dari input.
/// </summary>
public sealed class ProjectNode : PlanNode
{
    /// <summary>
    /// Input node (exactly one child).
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Daftar kolom yang akan di-output.
    /// </summary>
    public List<string> Columns { get; set; } = new();

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "PROJECT";
    public override string Details => $"Columns: {string.Join(", ", Columns)}";

    public ProjectNode(PlanNode input, List<string> columns)
    {
        Input = input;
        Columns = columns;
    }
}

/// <summary>
/// Unary Node: Sort - mengurutkan hasil berdasarkan kolom tertentu.
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

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "SORT";
    public override string Details => $"ORDER BY {string.Join(", ", OrderBy.Select(o => $"{o.Column} {(o.IsAscending ? "ASC" : "DESC")}"))}";

    public SortNode(PlanNode input, List<OrderByOperation> orderBy)
    {
        Input = input;
        OrderBy = orderBy;
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

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "AGGREGATE";
    public override string Details => $"GROUP BY {string.Join(", ", GroupBy)}";

    public AggregateNode(PlanNode input, List<string> groupBy)
    {
        Input = input;
        GroupBy = groupBy;
    }
}
