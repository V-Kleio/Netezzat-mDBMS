using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;
using System;
using System.Collections.Generic;

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
            var parsed = _queryOptimizer.ParseQuery(query);
            _queryOptimizer.OptimizeQuery(parsed);

            var upper = query.TrimStart().ToUpperInvariant();
            if (upper.StartsWith("SELECT"))
            {
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

            if (upper.StartsWith("INSERT") || upper.StartsWith("UPDATE"))
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

            if (upper.StartsWith("DELETE"))
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

            return new ExecutionResult()
            {
                Query = query,
                Success = false,
                Message = "Tipe DML query tidak dikenali atau belum didukung."
            };
        }
    }
}