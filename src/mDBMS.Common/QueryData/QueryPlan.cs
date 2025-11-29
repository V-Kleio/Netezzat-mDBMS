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
/// Representasi query plan
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
    /// Root node dari plan tree (NEW: tree-based representation).
    /// Jika ada, ini adalah representasi tree dari query plan.
    /// Steps (flat list) akan di-generate dari tree ini.
    /// </summary>
    public PlanNode? PlanTree { get; set; }

    /// <summary>
    /// Waktu pembuatan query plan
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}


