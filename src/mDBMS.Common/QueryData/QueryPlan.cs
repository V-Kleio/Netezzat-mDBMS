namespace mDBMS.Common.QueryData;

/// <summary>
/// Strategy optimisasi query
/// </summary>
public enum OptimizerStrategy {
    RULE_BASED,
    COST_BASED,
    HEURISTIC,
    ADAPTIVE
}

/// <summary>
/// Representasi query plan menggunakan tree-based PlanNode.
/// </summary>
public class QueryPlan {
    /// <summary>
    /// ID unik query plan
    /// </summary>
    public Guid PlanId { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Query asli
    /// </summary>
    public Query OriginalQuery { get; set; } = null!;

    /// <summary>
    /// Perkiraan total cost eksekusi query
    /// </summary>
    public double TotalEstimatedCost { get; set; }

    /// <summary>
    /// Perkiraan baris (rows) yang diproses
    /// </summary>
    public int EstimatedRows { get; set; }

    /// <summary>
    /// Strategi optimisasi yang dipakai
    /// </summary>
    public OptimizerStrategy Strategy { get; set; }

    /// <summary>
    /// Root node dari plan tree.
    /// Ini adalah satu-satunya representasi dari query plan.
    /// Query Processor akan traverse tree ini secara rekursif.
    /// </summary>
    public PlanNode? PlanTree { get; set; }

    /// <summary>
    /// Waktu pembuatan query plan
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

