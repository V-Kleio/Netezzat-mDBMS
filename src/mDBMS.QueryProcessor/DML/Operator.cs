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
    protected LocalTableStorage localTableStorage;
    public bool usePreviousTable = false;

    public Operator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
    {
        this.storageManager = storageManager;
        this.queryPlanStep = queryPlanStep;
        this.localTableStorage = localTableStorage;
    }

    /// <summary>
    /// Menghasilkan semua baris hasil operator ini.
    /// </summary>
    /// <returns>Enumerable semua baris</returns>
    public abstract IEnumerable<Row> GetRows();
}
