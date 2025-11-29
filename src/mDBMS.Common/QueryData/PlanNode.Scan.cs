using mDBMS.Common.Data;

namespace mDBMS.Common.QueryData;

/// <summary>
/// Leaf Node: Table Scan - membaca seluruh tabel secara sequential.
/// HANYA node ini (dan index nodes) yang menggunakan UseTable.
/// </summary>
public sealed class TableScanNode : PlanNode
{
    /// <summary>
    /// Nama tabel yang akan di-scan.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Leaf node tidak punya children, total cost = node cost saja.
    /// </summary>
    public override double TotalCost => NodeCost;

    public override string OperationName => "TABLE_SCAN";
    public override string Details => $"Table: {TableName}";
}

/// <summary>
/// Leaf Node: Index Scan - scan menggunakan index (membaca semua entries di index).
/// Lebih efisien dari table scan karena data terurut dan ukuran index lebih kecil.
/// </summary>
public sealed class IndexScanNode : PlanNode
{
    public string TableName { get; set; } = string.Empty;
    public string IndexColumn { get; set; } = string.Empty;

    /// <summary>
    /// Leaf node tidak punya children, total cost = node cost saja.
    /// </summary>
    public override double TotalCost => NodeCost;

    public override string OperationName => "INDEX_SCAN";
    public override string Details => $"Table: {TableName}, Index: {IndexColumn}";
}

/// <summary>
/// Leaf Node: Index Seek - pencarian spesifik menggunakan index dengan kondisi.
/// Paling efisien untuk query selektif (WHERE dengan equality/range pada indexed column).
/// </summary>
public sealed class IndexSeekNode : PlanNode
{
    public string TableName { get; set; } = string.Empty;
    public string IndexColumn { get; set; } = string.Empty;
    
    /// <summary>
    /// Kondisi pencarian yang akan dievaluasi menggunakan index.
    /// Contoh: "age > 25", "id = 100"
    /// </summary>
    public IEnumerable<Condition> SeekCondition { get; set; }

    /// <summary>
    /// Leaf node tidak punya children, total cost = node cost saja.
    /// </summary>
    public override double TotalCost => NodeCost;

    public override string OperationName => "INDEX_SEEK";
    public override string Details => $"Table: {TableName}, Index: {IndexColumn}, Seek: {SeekCondition}";
}
