using mDBMS.Common.Data;

namespace mDBMS.Common.QueryData;

/// <summary>
/// Leaf Node: Table Scan - membaca seluruh tabel secara sequential.
/// </summary>
public sealed class TableScanNode : PlanNode
{
    /// <summary>
    /// Nama tabel yang akan di-scan.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    public override double TotalCost => NodeCost;
    public override string OperationName => "TABLE_SCAN";
    public override string Details => $"Table: {TableName}";

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitTableScanNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitTableScanNode(this);
    }
}

/// <summary>
/// Leaf Node: Index Scan - scan menggunakan index (membaca semua entries di index).
/// </summary>
public sealed class IndexScanNode : PlanNode
{
    public string TableName { get; set; } = string.Empty;
    public string IndexColumn { get; set; } = string.Empty;

    public override double TotalCost => NodeCost;
    public override string OperationName => "INDEX_SCAN";
    public override string Details => $"Table: {TableName}, Index: {IndexColumn}";

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitIndexScanNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitIndexScanNode(this);
    }
}

/// <summary>
/// Leaf Node: Index Seek - pencarian spesifik menggunakan index dengan kondisi.
/// </summary>
public sealed class IndexSeekNode : PlanNode
{
    public string TableName { get; set; } = string.Empty;
    public string IndexColumn { get; set; } = string.Empty;
    
    /// <summary>
    /// Kondisi pencarian yang akan dievaluasi menggunakan index.
    /// </summary>
    public IEnumerable<Condition> SeekConditions { get; set; } = Array.Empty<Condition>();

    public override double TotalCost => NodeCost;
    public override string OperationName => "INDEX_SEEK";
    public override string Details => $"Table: {TableName}, Index: {IndexColumn}";

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitIndexSeekNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitIndexSeekNode(this);
    }
}
