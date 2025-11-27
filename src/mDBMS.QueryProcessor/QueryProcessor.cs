using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;
using mDBMS.QueryProcessor.DML;
using mDBMS.QueryProcessor.Transaction;

namespace mDBMS.QueryProcessor
{
    /// <summary>
    /// kelas utama Query Processor untuk fase 1: parsing sederhana lalu routing ke komponen lain.
    /// </summary>
    public class QueryProcessor : IQueryProcessor
    {
        private readonly IStorageManager _storageManager;
        private readonly IQueryOptimizer _queryOptimizer;
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager;

        private readonly Dictionary<QueryClassification, IQueryHandler> _handlers;

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

            _handlers = new Dictionary<QueryClassification, IQueryHandler>
            {
                { QueryClassification.Dml, new DMLHandler(_storageManager, _queryOptimizer, _concurrencyControlManager, _failureRecoveryManager) },
                { QueryClassification.TransactionBegin, new BeginTransactionHandler(_concurrencyControlManager) },
                { QueryClassification.TransactionCommit, new CommitTransactionHandler(_concurrencyControlManager, _failureRecoveryManager) },
                { QueryClassification.TransactionAbort, new AbortTransactionHandler(_concurrencyControlManager, _failureRecoveryManager) }
            };
        }

        public ExecutionResult ExecuteQuery(string query, int transactionId)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                return LogAndReturn(new ExecutionResult()
                {
                    Query = string.Empty,
                    Success = false,
                    Message = "Query tidak boleh kosong."
                });
            }

            var normalizedQuery = query.Trim();
            try
            {
                var classification = Classify(normalizedQuery);
                
                if (_handlers.TryGetValue(classification, out var handler))
                {
                    return LogAndReturn(handler.HandleQuery(normalizedQuery, transactionId));
                }

                return LogAndReturn(new ExecutionResult()
                {
                    Query = normalizedQuery,
                    Success = false,
                    Message = "Tipe query tidak dikenali atau belum didukung."
                });
            }
            catch (Exception ex)
            {
                return LogAndReturn(new ExecutionResult()
                {
                    Query = normalizedQuery,
                    Success = false,
                    Message = $"Terjadi kesalahan saat mengeksekusi query: {ex.Message}"
                });
            }
        }

        private ExecutionResult LogAndReturn(ExecutionResult result)
        {
            _failureRecoveryManager.WriteLog(result);
            return result;
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