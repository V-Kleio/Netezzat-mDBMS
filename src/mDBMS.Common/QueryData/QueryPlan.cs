namespace mDBMS.Common.QueryData;

/// <summary>
/// Tipe operasi pada query plan
/// </summary>
public enum OperationType {
    TABLE_SCAN,
    INDEX_SCAN,
    INDEX_SEEK,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    MERGE_JOIN,
    SORT,
    FILTER,
    PROJECTION,
    AGGREGATION
}

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
/// Step eksekusi dalam query plan
/// </summary>
public class QueryPlanStep {
    /// <summary>
    /// Urutan step
    /// </summary>
    public int Order { get; set; }

    /// <summary>
    /// Tipe operasi
    /// </summary>
    public OperationType Operation { get; set; }

    /// <summary>
    /// Deskripsi operasi
    /// </summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>
    /// Tabel yang dipakai
    /// </summary>
    public string Table { get; set; } = string.Empty;

    /// <summary>
    /// Index yang dipakai (jika ada)
    /// </summary>
    public string? IndexUsed { get; set; }

    /// <summary>
    /// Perkiraan cost operasi
    /// </summary>
    public double EstimatedCost { get; set; }
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
    /// Daftar step dalam query plan
    /// </summary>
    public List<QueryPlanStep> Steps { get; set; } = new List<QueryPlanStep>();

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
    /// Waktu pembuatan query plan
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}


