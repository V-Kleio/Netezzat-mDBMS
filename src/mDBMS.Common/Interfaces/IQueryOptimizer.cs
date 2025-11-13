using mDBMS.Common.QueryData;
namespace mDBMS.Common.Interfaces;

public interface IQueryOptimizer
{
    /// <summary>
    /// Melakukan parsing query string yang diberikan oleh user
    /// </summary>
    /// <param name="queryString">String query awal</param>
    /// <returns>Representasi pohon dari query</returns>
    public Query ParseQuery(string queryString);

    /// <summary>
    /// Mengoptimalkan query, mereturn query plan
    /// </summary>
    /// <param name="query">Query yang akan dioptimalkan</param>
    /// <returns>Optimized query execution plan</returns>
    QueryPlan OptimizeQuery(Query query);

    /// <summary>
    /// Menghitung perkiraan cost untuk query plan
    /// </summary>
    /// <param name="plan">Query plan</param>
    /// <returns>Estimasi cost dalam bentuk double floating number</returns>
    double GetCost(QueryPlan plan);

    /// <summary>
    /// Menggenerate beberapa alternatif query plan
    /// </summary>
    /// <param name="query">Query yang akan dianalisis</param>
    /// <returns>Daftar alternatif query plan</returns>
    IEnumerable<QueryPlan> GenerateAlternativePlans(Query query);
}
