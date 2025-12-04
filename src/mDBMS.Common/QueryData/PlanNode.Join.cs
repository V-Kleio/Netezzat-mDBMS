using mDBMS.Common.Data;

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
    /// </summary>
    public Condition JoinCondition { get; set; }

    /// <summary>
    /// Algoritma yang digunakan untuk join.
    /// </summary>
    public JoinAlgorithm Algorithm { get; set; } = JoinAlgorithm.NestedLoop;

    public override double TotalCost => NodeCost + Left.TotalCost + Right.TotalCost;
    public override string OperationName => $"{JoinType}_{Algorithm}_JOIN";
    public override string Details => $"ON {JoinCondition}";

    public JoinNode(PlanNode left, PlanNode right, JoinType joinType, Condition condition)
    {
        Left = left;
        Right = right;
        JoinType = joinType;
        JoinCondition = condition;
    }

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitJoinNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitJoinNode(this);
    }
}

/// <summary>
/// Algoritma join yang tersedia.
/// </summary>
public enum JoinAlgorithm
{
    NestedLoop,
    Hash,
    Merge
}
