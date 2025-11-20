using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class LocalTableStorage
{
    /// <summary>
    /// Penyimpanan hasil sementara operator yang belum digunakan
    /// </summary>
    public IEnumerable<Row> holdStorage = [];

    /// <summary>
    /// Penyimpanan hasil terakhir dari eksekusi operator
    /// </summary>
    public IEnumerable<Row> lastResult = [];
}