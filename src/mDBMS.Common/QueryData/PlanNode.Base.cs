namespace mDBMS.Common.QueryData;

/// <summary>
/// Node dasar untuk representasi tree dari query execution plan.
/// Setiap node merepresentasikan satu operasi dalam eksekusi query.
/// Tree structure memudahkan traversal, transformation, dan cost calculation.
/// 
/// Query Processor akan traverse tree ini secara rekursif menggunakan pattern matching.
/// </summary>
public abstract class PlanNode
{
    /// <summary>
    /// Estimasi jumlah baris yang dihasilkan node ini.
    /// Digunakan sebagai input untuk cost calculation node parent.
    /// </summary>
    public double EstimatedRows { get; set; }

    /// <summary>
    /// Cost untuk mengeksekusi operasi di node ini saja (tidak termasuk children).
    /// Dihitung oleh ICostModel berdasarkan jenis operasi dan input size.
    /// </summary>
    public double NodeCost { get; set; }

    /// <summary>
    /// Total cost untuk mengeksekusi node ini dan semua children-nya.
    /// Computed secara recursive dari bottom-up.
    /// Setiap subclass override untuk menghitung berdasarkan struktur children-nya.
    /// </summary>
    public abstract double TotalCost { get; }

    /// <summary>
    /// Nama operasi yang dilakukan node ini (untuk display/debugging).
    /// </summary>
    public abstract string OperationName { get; }

    /// <summary>
    /// Detail tambahan untuk debugging/explain plan.
    /// Contoh: "Table: employees", "Condition: age > 25"
    /// </summary>
    public virtual string Details => string.Empty;

    /// <summary>
    /// Representasi string untuk debugging.
    /// Format: "OperationName (rows=X, cost=Y) Details"
    /// </summary>
    public override string ToString()
    {
        var details = string.IsNullOrEmpty(Details) ? "" : $" - {Details}";
        return $"{OperationName} (rows={EstimatedRows:F0}, cost={NodeCost:F2}){details}";
    }

    /// <summary>
    /// Konversi node tree menjadi QueryPlan untuk interface IQueryOptimizer.
    /// </summary>
    public QueryPlan ToQueryPlan()
    {
        return new QueryPlan
        {
            TotalEstimatedCost = TotalCost,
            EstimatedRows = (int)EstimatedRows,
            Strategy = OptimizerStrategy.COST_BASED,
            PlanTree = this
        };
    }
}
