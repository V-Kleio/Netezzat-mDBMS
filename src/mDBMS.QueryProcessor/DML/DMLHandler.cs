using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Transaction;

// TODO: Get therapy

namespace mDBMS.QueryProcessor.DML
{
    internal class DMLHandler : IQueryHandler
    {
        private readonly IStorageManager _storageManager;
        private readonly IQueryOptimizer _queryOptimizer;
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager;

        public DMLHandler(
            IStorageManager storageManager,
            IQueryOptimizer queryOptimizer,
            IConcurrencyControlManager concurrencyControlManager,
            IFailureRecoveryManager failureRecoveryManager
        ) {
            _storageManager = storageManager;
            _queryOptimizer = queryOptimizer;
            _concurrencyControlManager = concurrencyControlManager;
            _failureRecoveryManager = failureRecoveryManager;
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            string upper = query.Split()[0].Trim().ToUpperInvariant();
            return upper switch
            {
                "SELECT" => HandleSelect(query, transactionId),
                "INSERT" => HandleInsert(query, transactionId),
                "UPDATE" => HandleUpdate(query, transactionId),
                "DELETE" => HandleDelete(query, transactionId),
                _ => HandleUnrecognized(query, transactionId)
            };
        }

        private ExecutionResult HandleSelect(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // Meminta izin read dari Concurrency Control Manager
            var action = new Common.Transaction.Action(
                Common.Transaction.Action.ActionType.Read,
                DatabaseObject.CreateRow("ANY", parsedQuery.Table),
                transactionId,
                query
            );

            // validasi ke Concurrency Control Manager
            var response = _concurrencyControlManager.ValidateObject(action);

            if (!response.Allowed)
            {
                _concurrencyControlManager.AbortTransaction(transactionId);

                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Read operation ditolak oleh CCM: {response.Reason}",
                    TransactionId = transactionId
                };
            }

            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);

            LocalTableStorage storage = new LocalTableStorage();

            foreach (QueryPlanStep step in queryPlan.Steps)
            {
                Operator? op = step.Operation switch
                {
                    OperationType.TABLE_SCAN => new TableScanOperator(_storageManager, step, storage),
                    OperationType.INDEX_SCAN => new IndexScanOperator(_storageManager, step, storage),
                    OperationType.INDEX_SEEK => new IndexSeekOperator(_storageManager, step, storage),
                    OperationType.FILTER => new FilterOperator(_storageManager, step, storage),
                    OperationType.PROJECTION => new ProjectionOperator(_storageManager, step, storage),
                    OperationType.NESTED_LOOP_JOIN => new NestedLoopJoinOperator(_storageManager, step, storage),
                    OperationType.HASH_JOIN => new HashJoinOperator(_storageManager, step, storage),
                    OperationType.MERGE_JOIN => new MergeJoinOperator(_storageManager, step, storage),
                    OperationType.SORT => new MergeSortOperator(_storageManager, step, storage),
                    _ => null
                };

                if (op == null) return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Operasi {step.Operation} untuk SELECT tidak didukung.",
                    TransactionId = transactionId
                };

                IEnumerable<Row> result = op.GetRows();

                LocalTableStorage oldStorage = storage;
                storage = new LocalTableStorage();

                if (!op.usePreviousTable)
                {
                    storage.holdStorage = oldStorage.lastResult;
                }

                storage.lastResult = result;
            }

            // SELECT tidak perlu dilog karena tidak mengubah data, tapi tetap isi TransactionId
            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"",
                Data = storage.lastResult,
                TransactionId = transactionId
            };
        }

        private ExecutionResult HandleInsert(string query, int transactionId)
        {
            var data = new Dictionary<string, object>
            {
                ["example_col"] = "value"
            };

            // ekstrak nama tabel dari query
            string tableName = ExtractTableNameFromQuery(query, "INSERT");

            var write = new DataWrite(tableName, data);
            var affected = _storageManager.WriteBlock(write);

            // untuk INSERT, AfterImage adalah data yang baru diinsert
            // BeforeImage null karena data belum ada sebelumnya
            var afterImage = SerializeData(data);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affected} row(s) ditulis/diperbarui melalui Storage Manager.",
                TransactionId = transactionId,
                TableName = tableName,
                AfterImage = afterImage,
                BeforeImage = null,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleUpdate(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // At this point I don't care about making beautiful code anymore
            // I'm just gonna do what works (compiles)

            Condition? condition = null;

            if (parsedQuery.WhereClause is not null)
            {
                var operations = new Dictionary<string, Condition.Operation>()
                {
                    ["<"] = Condition.Operation.LT,
                    [">"] = Condition.Operation.GT,
                    ["="] = Condition.Operation.EQ,
                    ["!="] = Condition.Operation.NEQ,
                    ["<>"] = Condition.Operation.NEQ,
                    [">="] = Condition.Operation.GEQ,
                    ["<="] = Condition.Operation.LEQ,
                };

                foreach ((string opchar, var operation) in operations)
                {
                    string[] operands = parsedQuery.WhereClause.Split(opchar, 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

                    if (operands.Length < 2) continue;

                    foreach (string operand in operands)
                    {
                        if (!operand.All(c => char.IsAsciiLetterOrDigit(c) || c == '.' || c == '_'))
                        {
                            return new();
                        }
                    }

                    condition = new()
                    {
                        lhs = operands[0],
                        rhs = operands[1],
                        opr = operation
                    };

                    break;
                }
            }

            Dictionary<string, object> newValues = new();
            foreach ((var key, var val) in parsedQuery.UpdateOperations)
            {
                if (int.TryParse(val, out int intval))
                {
                    newValues.Add(key, intval);
                }
                else if (float.TryParse(val, out float floatval))
                {
                    newValues.Add(key, floatval);
                }
                else
                {
                    newValues.Add(key, val);
                }
            }

            // Wtf is serialize data doing????
            // Why are we only serializing the first rows to log????
            // Why are there 2 functions????

            DataRetrieval readRequest = new DataRetrieval(parsedQuery.Table, [], condition);
            var beforeImage = SerializeRowData(_storageManager.ReadBlock(readRequest).ToList().First());

            // We must now lock every row...
            // How does one procure the row id?
            for (int i = 0; i < beforeImage.Length; i++)
            {
                var action = new Common.Transaction.Action(
                    Common.Transaction.Action.ActionType.Write,
                    DatabaseObject.CreateRow("temp", parsedQuery.Table),
                    transactionId, query
                );

                var response = _concurrencyControlManager.ValidateObject(action);

                if (!response.Allowed)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);
                    return new()
                    {
                        Query = query,
                        Success = false,
                        Message = "Could not validate transaction operation: " + response.Reason
                    };
                }
            }

            DataWrite writeRequest = new(parsedQuery.Table, newValues, condition);
            int affectedRowCount = _storageManager.WriteBlock(writeRequest);
            var afterImage = SerializeData(_storageManager.ReadBlock(readRequest).ToList().First().Columns);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affectedRowCount} row(s) ditulis/diperbarui melalui Storage Manager.",
                TransactionId = transactionId,
                TableName = parsedQuery.Table,
                BeforeImage = beforeImage,
                AfterImage = afterImage,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleDelete(string query, int transactionId)
        {
            // ekstrak nama tabel dari query
            string tableName = ExtractTableNameFromQuery(query, "DELETE");

            // untuk DELETE, harus baca data yang akan dihapus terlebih dahulu (BeforeImage)
            var retrieval = new DataRetrieval(tableName, new[] { "*" });
            var beforeRows = _storageManager.ReadBlock(retrieval).ToList();

            var deletion = new DataDeletion(tableName);
            var deleted = _storageManager.DeleteBlock(deletion);

            // untuk DELETE, BeforeImage adalah data yang akan dihapus
            // AfterImage null karena data sudah tidak ada setelah delete
            string? beforeImage = beforeRows.Any() ? SerializeRowData(beforeRows.First()) : null;

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{deleted} row(s) dihapus melalui Storage Manager.",
                TransactionId = transactionId,
                TableName = tableName,
                BeforeImage = beforeImage,
                AfterImage = null,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleUnrecognized(string query, int transactionId)
        {
            return new ExecutionResult()
            {
                Query = query,
                Success = false,
                Message = "Tipe DML query tidak dikenali atau belum didukung.",
                TransactionId = transactionId
            };
        }

        /// <summary>
        /// ekstrak nama tabel dari query string (simple parser)
        /// </summary>
        private string ExtractTableNameFromQuery(string query, string operationType)
        {
            var tokens = query.Trim().Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

            if (operationType == "INSERT")
            {
                // INSERT INTO table_name ...
                if (tokens.Length > 2 && tokens[0].Equals("INSERT", StringComparison.OrdinalIgnoreCase)
                    && tokens[1].Equals("INTO", StringComparison.OrdinalIgnoreCase))
                {
                    return tokens[2];
                }
            }
            else if (operationType == "UPDATE")
            {
                // UPDATE table_name ...
                if (tokens.Length > 1 && tokens[0].Equals("UPDATE", StringComparison.OrdinalIgnoreCase))
                {
                    return tokens[1];
                }
            }
            else if (operationType == "DELETE")
            {
                // DELETE FROM table_name ...
                if (tokens.Length > 2 && tokens[0].Equals("DELETE", StringComparison.OrdinalIgnoreCase)
                    && tokens[1].Equals("FROM", StringComparison.OrdinalIgnoreCase))
                {
                    return tokens[2];
                }
            }

            return "UNKNOWN";
        }

        /// <summary>
        /// serialize dictionary data menjadi string format JSON-like
        /// </summary>
        private string SerializeData(Dictionary<string, object> data)
        {
            var columns = data.Select(kv => $"\"{kv.Key}\":\"{kv.Value}\"");
            return "{" + string.Join(",", columns) + "}";
        }

        /// <summary>
        /// serialize row data menjadi string format JSON-like
        /// </summary>
        private string SerializeRowData(Row row)
        {
            var columns = row.Columns.Select(kv => $"\"{kv.Key}\":\"{kv.Value}\"");
            return "{" + string.Join(",", columns) + "}";
        }
    }
}