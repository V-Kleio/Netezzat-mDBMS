using mDBMS.Common.Transaction;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;

namespace mDBMS.QueryProcessor
{
    /// <summary>
    /// kelas utama Query Processor untuk fase 1: parsing sederhana lalu routing ke komponen lain.
    /// </summary>
    public class QueryProcessor
    {
        private readonly IStorageManager _storageManager;
        private readonly IQueryOptimizer _queryOptimizer;
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager;

        private int? _activeTransactionId;

        public QueryProcessor(
            IStorageManager storageManager,
            IQueryOptimizer queryOptimizer,
            IConcurrencyControlManager concurrencyControlManager,
            IFailureRecoveryManager failureRecoveryManager)
        {
            _storageManager = storageManager ?? throw new ArgumentNullException(nameof(storageManager));
            _queryOptimizer = queryOptimizer ?? throw new ArgumentNullException(nameof(queryOptimizer));
            _concurrencyControlManager = concurrencyControlManager ?? throw new ArgumentNullException(nameof(concurrencyControlManager));
            _failureRecoveryManager = failureRecoveryManager ?? throw new ArgumentNullException(nameof(failureRecoveryManager));
        }

        public ExecutionResult ExecuteQuery(string? query)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                return LogAndReturn(BuildResult(string.Empty, success: false, message: "Query tidak boleh kosong."));
            }

            var normalizedQuery = query.Trim();
            try
            {
                var classification = Classify(normalizedQuery);
                var result = classification switch
                {
                    QueryClassification.Dml => HandleDmlQuery(normalizedQuery),
                    QueryClassification.TransactionBegin => HandleBeginTransaction(normalizedQuery),
                    QueryClassification.TransactionCommit => HandleCommitTransaction(normalizedQuery),
                    QueryClassification.TransactionAbort => HandleAbortTransaction(normalizedQuery),
                    _ => BuildResult(normalizedQuery, false, "Perintah tidak dikenali. Tulis SELECT/INSERT/UPDATE/DELETE atau perintah transaksi."),
                };

                return LogAndReturn(result);
            }
            catch (Exception ex)
            {
                return LogAndReturn(BuildResult(normalizedQuery, false, $"Kesalahan internal Query Processor: {ex.Message}"));
            }
        }

        private ExecutionResult HandleDmlQuery(string query)
        {
            var parsed = _queryOptimizer.ParseQuery(query);
            _queryOptimizer.OptimizeQuery(parsed);

            var upper = query.TrimStart().ToUpperInvariant();
            if (upper.StartsWith("SELECT"))
            {
                var retrieval = new DataRetrieval("employee", new[] { "*" });
                var rows = _storageManager.ReadBlock(retrieval);

                return BuildResult(query, true, "Data berhasil diambil melalui Storage Manager.", rows);
            }

            if (upper.StartsWith("INSERT") || upper.StartsWith("UPDATE"))
            {
                var data = new Dictionary<string, object>
                {
                    ["example_col"] = "value"
                };

                var write = new DataWrite("employee", data);
                var affected = _storageManager.WriteBlock(write);

                return BuildResult(query, true, $"{affected} row(s) ditulis melalui Storage Manager.");
            }

            if (upper.StartsWith("DELETE"))
            {
                var deletion = new DataDeletion("employee");
                var deleted = _storageManager.DeleteBlock(deletion);

                return BuildResult(query, true, $"{deleted} row(s) dihapus melalui Storage Manager.");
            }

            return BuildResult(query, true, "Query berhasil diparse dan diteruskan ke Query Optimizer.");
        }

        private ExecutionResult HandleBeginTransaction(string query)
        {
            if (_activeTransactionId.HasValue)
            {
                return BuildResult(query, false, $"Masih ada transaksi aktif dengan ID {_activeTransactionId.Value}.");
            }

            _activeTransactionId = _concurrencyControlManager.BeginTransaction();
            return BuildResult(query, true, $"Transaksi baru dimulai dengan ID {_activeTransactionId.Value}.");
        }

        private ExecutionResult HandleCommitTransaction(string query)
        {
            if (!_activeTransactionId.HasValue)
            {
                return BuildResult(query, false, "Tidak ada transaksi aktif yang bisa di-COMMIT.");
            }

            var transactionId = _activeTransactionId.Value;
            _concurrencyControlManager.EndTransaction(transactionId, true);
            _activeTransactionId = null;
            return BuildResult(query, true, $"Transaksi {transactionId} berhasil di-COMMIT.");
        }

        private ExecutionResult HandleAbortTransaction(string query)
        {
            if (!_activeTransactionId.HasValue)
            {
                return BuildResult(query, false, "Tidak ada transaksi aktif yang bisa di-ABORT.");
            }

            var transactionId = _activeTransactionId.Value;
            _concurrencyControlManager.EndTransaction(transactionId, false);
            _activeTransactionId = null;
            return BuildResult(query, true, $"Transaksi {transactionId} berhasil di-ABORT.");
        }

        private ExecutionResult LogAndReturn(ExecutionResult result)
        {
            _failureRecoveryManager.WriteLog(result);
            return result;
        }

        // untuk result bukan select
        private static ExecutionResult BuildResult(string query, bool success, string message)
        {
            return new ExecutionResult
            {
                Query = query,
                Success = success,
                Message = message

            };
        }

        // khusus untuk result select
        private static ExecutionResult BuildResult(string query, bool success, string message, IEnumerable<Row> data)
        {
            return new ExecutionResult
            {
                Query = query,
                Success = success,
                Message = message,
                Data = data
            };
        }

        private static QueryClassification Classify(string query)
        {
            var upper = query.TrimStart().ToUpperInvariant();

            if (upper.StartsWith("BEGIN"))
            {
                return QueryClassification.TransactionBegin;
            }

            if (upper.StartsWith("COMMIT"))
            {
                return QueryClassification.TransactionCommit;
            }

            if (upper.StartsWith("ROLLBACK") || upper.StartsWith("ABORT"))
            {
                return QueryClassification.TransactionAbort;
            }

            if (upper.StartsWith("SELECT") ||
                upper.StartsWith("INSERT") ||
                upper.StartsWith("UPDATE") ||
                upper.StartsWith("DELETE"))
            {
                return QueryClassification.Dml;
            }

            return QueryClassification.Unknown;
        }

        private enum QueryClassification
        {
            Unknown = 0,
            Dml,
            TransactionBegin,
            TransactionCommit,
            TransactionAbort
        }
    }
}
