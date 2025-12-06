namespace mDBMS.Common.QueryData;

/// <summary>
/// Enum type operasi query
/// </summary>
public enum QueryType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE
}

/// <summary>
/// Tipe operasi JOIN
/// </summary>
public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS
}

/// <summary>
/// Operasi JOIN
/// </summary>
public class JoinOperation {
    public string LeftTable { get; set; } = string.Empty;
    public string RightTable { get; set; } = string.Empty;
    public string OnCondition { get; set; } = string.Empty;
    public JoinType Type { get; set; } = JoinType.INNER;
}

/// <summary>
/// Operasi ORDER BY
/// </summary>
public class OrderByOperation {
    public string Column { get; set; } = string.Empty;
    public bool IsAscending { get; set; } = true;
}


public class Query {
    /// <summary>
    /// Tabel query target
    /// </summary>
    public string Table { get; set; } = string.Empty;
    public List<string>? FromTables { get; set; }

    /// <summary>
    /// Kolom yang dipilih
    /// </summary>
    public List<string> SelectedColumns { get; set; } = new List<string>();

    ///<ummary>
    /// Mapping kolom dan nilai baru untuk UPDATE
    /// </summary>
    public Dictionary<string, string> UpdateOperations { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// Kondisi WHERE
    /// </summary>
    public string? WhereClause { get; set; }

    /// <summary>
    /// Operasi Join
    /// </summary>
    public List<JoinOperation>? Joins { get; set; }

    /// <summary>
    /// Operasi Order By
    /// </summary>
    public List<OrderByOperation>? OrderBy { get; set; }

    /// <summary>
    /// Operasi Group By
    /// </summary>
    public List<string>? GroupBy { get; set; } = new List<string>();

    /// <summary>
    /// Tipe operasi query (SELECT, INSERT, UPDATE, DELETE)
    /// </summary>
    public QueryType Type { get; set; } = QueryType.SELECT;

    /// <summary>
    /// Kolom target untuk INSERT (optional, null jika semua kolom).
    /// </summary>
    public List<string>? InsertColumns { get; set; }

    /// <summary>
    /// List of value lists untuk multi-row INSERT. Setiap inner list adalah satu row values.
    /// </summary>
    public List<List<string>>? InsertValues { get; set; }

    /// <summary>
    /// Query sumber untuk INSERT ... SELECT. Jika tidak null, maka InsertValues harus null.
    /// </summary>
    public Query? InsertFromQuery { get; set; }
}


