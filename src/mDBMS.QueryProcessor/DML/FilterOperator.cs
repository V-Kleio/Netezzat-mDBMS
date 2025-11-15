using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class FilterOperator : Operator
{
    public FilterOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
    }

    public override IEnumerable<Row> GetRows()
    {
        yield return null!;
    }
}