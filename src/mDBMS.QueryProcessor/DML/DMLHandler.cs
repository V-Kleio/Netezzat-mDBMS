using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
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
            string upper = query.Split()[0].Trim().ToUpperInvariant();
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
            Query parsedQuery = _queryOptimizer.ParseQuery(query);
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);

            LocalTableStorage storage = new LocalTableStorage();

            foreach (QueryPlanStep step in queryPlan.Steps)
            {
                Operator? op = step.Operation switch
                {
                    OperationType.TABLE_SCAN => new TableScanOperator(_storageManager, step, storage),
                    // OperationType.INDEX_SCAN
                    // OperationType.INDEX_SEEK
                    OperationType.FILTER => new FilterOperator(_storageManager, step, storage),
                    OperationType.PROJECTION => new ProjectionOperator(_storageManager, step, storage),
                    OperationType.NESTED_LOOP_JOIN => new NestedLoopJoinOperator(_storageManager, step, storage),
                    OperationType.HASH_JOIN => new HashJoinOperator(_storageManager, step, storage),
                    // OperationType.MERGE_JOIN
                    // OperationType.SORT
                    // OperationType.AGGREGATION
                    _ => null
                };

                if (op == null) return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Operasi {step.Operation} untuk SELECT tidak didukung."
                };

                IEnumerable<Row> result = op.GetRows();

                if (!op.usePreviousTable)
                {
                    storage.holdStorage = storage.lastResult;
                }

                storage.lastResult = result;
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"",
                Data = storage.lastResult
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