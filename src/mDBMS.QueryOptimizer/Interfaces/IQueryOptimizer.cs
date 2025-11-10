using mDBMS.QueryOptimizer.Models;

namespace mDBMS.QueryOptimizer.Interfaces
{
    /// <summary>
    /// Interface untuk Query Optimizer
    /// </summary>
    public interface IQueryOptimizer
    {
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
}