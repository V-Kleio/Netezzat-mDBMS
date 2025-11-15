using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class TableScanOperator : Operator
{
    public TableScanOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep)
        : base(storageManager, queryPlanStep)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
    }

    public override IEnumerable<Row> GetRows()
    {
        yield return null!;
    }
}