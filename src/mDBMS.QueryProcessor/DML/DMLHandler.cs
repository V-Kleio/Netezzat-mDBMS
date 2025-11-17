using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.DML
{
    internal class DMLHandler : IQueryHandler
    {
        private readonly IStorageManager _storageManager;
        private readonly IQueryOptimizer _queryOptimizer;

        public DMLHandler(IStorageManager storageManager, IQueryOptimizer queryOptimizer)
        {
            _storageManager = storageManager;
            _queryOptimizer = queryOptimizer;
        }

        public ExecutionResult HandleQuery(string query)
        {
            string upper = query.TrimStart().ToUpperInvariant();
            return upper switch
            {
                "SELECT" => HandleSelect(query),
                "INSERT" => HandleInsert(query),
                "UPDATE" => HandleUpdate(query),
                "DELETE" => HandleDelete(query),
                _ => HandleUnrecognized(query)
            };
        }

        private ExecutionResult HandleSelect(string query)
        {
            var parsed = _queryOptimizer.ParseQuery(query);
            _queryOptimizer.OptimizeQuery(parsed);

            var retrieval = new DataRetrieval("employee", new[] { "*" });
            var rows = _storageManager.ReadBlock(retrieval);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = "Data berhasil diambil melalui Storage Manager.",
                Data = rows
            };
        }

        private ExecutionResult HandleInsert(string query)
        {
            var data = new Dictionary<string, object>
            {
                ["example_col"] = "value"
            };

            var write = new DataWrite("employee", data);
            var affected = _storageManager.WriteBlock(write);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affected} row(s) ditulis/diperbarui melalui Storage Manager."
            };
        }

        private ExecutionResult HandleUpdate(string query)
        {
            var data = new Dictionary<string, object>
            {
                ["example_col"] = "value"
            };

            var write = new DataWrite("employee", data);
            var affected = _storageManager.WriteBlock(write);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affected} row(s) ditulis/diperbarui melalui Storage Manager."
            };
        }

        private ExecutionResult HandleDelete(string query)
        {
            var deletion = new DataDeletion("employee");
            var deleted = _storageManager.DeleteBlock(deletion);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{deleted} row(s) dihapus melalui Storage Manager."
            };
        }

        private ExecutionResult HandleUnrecognized(string query)
        {
            return new ExecutionResult()
            {
                Query = query,
                Success = false,
                Message = "Tipe DML query tidak dikenali atau belum didukung."
            };
        }
    }
}