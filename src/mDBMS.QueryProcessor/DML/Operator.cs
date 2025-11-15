using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
namespace mDBMS.QueryProcessor.DML;

/// <summary>
/// Kelas dasar untuk semua operator dalam pemrosesan query DML.
/// </summary>
internal abstract class Operator
{
    protected IStorageManager storageManager;
    protected QueryPlanStep queryPlanStep;

    public Operator(IStorageManager storageManager, QueryPlanStep queryPlanStep)
    {
        this.storageManager = storageManager;
        this.queryPlanStep = queryPlanStep;
    }

    /// <summary>
    /// Menghasilkan semua baris hasil operator ini.
    /// </summary>
    /// <returns>Enumerable semua baris</returns>
    public abstract IEnumerable<Row> GetRows();
}
