using mDBMS.Common.Data;

namespace mDBMS.Common.QueryData;

/// <summary>
/// DML Node: Update - menjalankan UPDATE pada tabel.
/// </summary>
public sealed class UpdateNode : PlanNode
{
    /// <summary>
    /// Input node yang menghasilkan rows untuk di-update.
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Nama tabel yang akan di-update.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Dictionary dari kolom yang akan diupdate beserta nilai barunya.
    /// Key: column name (qualified), Value: new value
    /// </summary>
    public Dictionary<string, string> UpdateOperations { get; set; } = new();

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "UPDATE";
    public override string Details => $"Table: {TableName}, Updates: {UpdateOperations.Count} column(s)";

    public UpdateNode(PlanNode input)
    {
        Input = input;
    }

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitUpdateNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitUpdateNode(this);
    }
}

/// <summary>
/// DML Node: Insert - menjalankan INSERT pada tabel.
/// </summary>
public sealed class InsertNode : PlanNode
{
    /// <summary>
    /// Nama tabel target insert.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Kolom-kolom yang akan diisi.
    /// </summary>
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// Nilai-nilai untuk di-insert (parallel dengan Columns).
    /// </summary>
    public List<string> Values { get; set; } = new();

    public override double TotalCost => NodeCost;
    public override string OperationName => "INSERT";
    public override string Details => $"Table: {TableName}, Columns: {string.Join(", ", Columns)}";

    public InsertNode()
    {
    }

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitInsertNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitInsertNode(this);
    }
}

/// <summary>
/// DML Node: Delete - menjalankan DELETE pada tabel.
/// </summary>
public sealed class DeleteNode : PlanNode
{
    /// <summary>
    /// Input node yang menghasilkan rows untuk di-delete.
    /// </summary>
    public PlanNode Input { get; }

    /// <summary>
    /// Nama tabel yang akan di-delete.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    public override double TotalCost => NodeCost + Input.TotalCost;
    public override string OperationName => "DELETE";
    public override string Details => $"Table: {TableName}";

    public DeleteNode(PlanNode input)
    {
        Input = input;
    }

    public override R AcceptVisitor<R>(IPlanNodeVisitor<R> visitor)
    {
        return visitor.VisitDeleteNode(this);
    }

    public override void AcceptVisitor(IPlanNodeVisitor visitor)
    {
        visitor.VisitDeleteNode(this);
    }
}
