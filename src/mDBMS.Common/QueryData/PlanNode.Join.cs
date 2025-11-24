namespace mDBMS.Common.QueryData;

/// <summary>
/// Binary Node: Join - menggabungkan dua input berdasarkan kondisi.
/// </summary>
public sealed class JoinNode : PlanNode
{
    /// <summary>
    /// Left child (outer table).
    /// </summary>
    public PlanNode Left { get; }

    /// <summary>
    /// Right child (inner table).
    /// </summary>
    public PlanNode Right { get; }

    /// <summary>
    /// Jenis join (INNER, LEFT, RIGHT, FULL).
    /// </summary>
    public JoinType JoinType { get; set; } = JoinType.INNER;

    /// <summary>
    /// Kondisi join (ON clause).
    /// Contoh: "e.dept_id = d.id"
    /// </summary>
    public string JoinCondition { get; set; } = string.Empty;

    /// <summary>
    /// Algoritma yang digunakan untuk join.
    /// </summary>
    public JoinAlgorithm Algorithm { get; set; } = JoinAlgorithm.NestedLoop;

    /// <summary>
    /// Total cost = node cost + left cost + right cost.
    /// </summary>
    public override double TotalCost => NodeCost + Left.TotalCost + Right.TotalCost;

    public override string OperationName => $"{JoinType}_JOIN ({Algorithm})";
    public override string Details => $"ON {JoinCondition}";

    public JoinNode(PlanNode left, PlanNode right, JoinType joinType, string condition)
    {
        Left = left;
        Right = right;
        JoinType = joinType;
        JoinCondition = condition;
    }

    public override List<QueryPlanStep> ToSteps()
    {
        // Gabungkan steps dari kedua children
        var leftSteps = Left.ToSteps();
        var rightSteps = Right.ToSteps();

        // Renumber right steps
        foreach (var step in rightSteps)
        {
            step.Order += leftSteps.Count;
        }

        var allSteps = new List<QueryPlanStep>();
        allSteps.AddRange(leftSteps);
        allSteps.AddRange(rightSteps);

        // Tambahkan join step
        allSteps.Add(new QueryPlanStep
        {
            Order = allSteps.Count + 1,
            Operation = Algorithm switch
            {
                JoinAlgorithm.NestedLoop => OperationType.NESTED_LOOP_JOIN,
                JoinAlgorithm.Hash => OperationType.HASH_JOIN,
                JoinAlgorithm.Merge => OperationType.MERGE_JOIN,
                _ => OperationType.NESTED_LOOP_JOIN
            },
            Description = Details,
            EstimatedCost = NodeCost
        });

        return allSteps;
    }
}

/// <summary>
/// Algoritma join yang tersedia.
/// </summary>
public enum JoinAlgorithm
{
    /// <summary>
    /// Nested Loop: untuk setiap row di left, scan semua row di right.
    /// Cocok untuk small tables atau jika right table punya index.
    /// </summary>
    NestedLoop,

    /// <summary>
    /// Hash Join: build hash table dari smaller table, probe dari larger.
    /// Cocok untuk equi-join pada large tables tanpa index.
    /// </summary>
    Hash,

    /// <summary>
    /// Merge Join: merge dua input yang sudah sorted.
    /// Cocok jika kedua input sudah terurut atau ada index.
    /// </summary>
    Merge
}
