using mDBMS.Common.Models;
namespace mDBMS.Common.Interfaces;

public interface IQueryOptimizer
{
    /// <summary>
    /// Melakukan parsing query string yang diberikan oleh user
    /// </summary>
    /// <param name="queryString">String query awal</param>
    /// <returns>Representasi pohon dari query</returns>
    public ISelection ParseQuery(string queryString);

    /// <summary>
    /// Mengoptimasi representasi pohon query dengan statistik yang telah diberikan
    /// </summary>
    /// <param name="query">Representasi pohon query yang belum dioptimasi</param>
    /// <param name="statistics">Daftar statistics tiap tabel yang digunakan dalam query</param>
    /// <returns>Representasi pohon dari query</returns>
    public ISelection OptimizeQuery(ISelection query, IEnumerable<Statistic> statistics);
}