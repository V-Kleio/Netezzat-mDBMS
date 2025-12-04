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
    /// [DEPRECATED] Linear list of query plan steps for backward compatibility.
    /// Use PlanTree instead for new code.
    /// </summary>
    [Obsolete("Use PlanTree instead. Steps will be removed in future versions.")]
    public List<QueryPlanStep> Steps { get; set; } = new();

    /// <summary>
    /// Waktu pembuatan query plan
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// [DEPRECATED] Tipe operasi dalam query plan step.
/// Use PlanNode types instead (TableScanNode, FilterNode, etc).
/// </summary>
[Obsolete("Use PlanNode types instead. OperationType will be removed in future versions.")]
public enum OperationType
{
    TABLE_SCAN,
    INDEX_SCAN,
    INDEX_SEEK,
    FILTER,
    PROJECTION,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    MERGE_JOIN,
    SORT,
    AGGREGATION,
    UPDATE,
    INSERT,
    DELETE,
    INDEX_MAINTENANCE
}

/// <summary>
/// [DEPRECATED] Representasi satu langkah dalam query plan.
/// Use PlanNode tree instead for new code.
/// </summary>
[Obsolete("Use PlanNode tree instead. QueryPlanStep will be removed in future versions.")]
public class QueryPlanStep
{
    /// <summary>
    /// Urutan eksekusi step
    /// </summary>
    public int Order { get; set; }

    /// <summary>
    /// Tipe operasi
    /// </summary>
    public OperationType Operation { get; set; }

    /// <summary>
    /// Deskripsi human-readable dari step ini
    /// </summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>
    /// Nama tabel yang terlibat
    /// </summary>
    public string Table { get; set; } = string.Empty;

    /// <summary>
    /// Index yang digunakan (jika ada)
    /// </summary>
    public string? IndexUsed { get; set; }

    /// <summary>
    /// Estimasi cost untuk step ini
    /// </summary>
    public double EstimatedCost { get; set; }

    /// <summary>
    /// Parameter-parameter tambahan untuk step ini
    /// </summary>
    public Dictionary<string, object?> Parameters { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

