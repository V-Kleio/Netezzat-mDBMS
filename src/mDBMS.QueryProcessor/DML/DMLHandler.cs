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
        private readonly QueryProcessor _processor;

        public DMLHandler(IStorageManager storageManager, IQueryOptimizer queryOptimizer, QueryProcessor processor)
        {
            _storageManager = storageManager;
            _queryOptimizer = queryOptimizer;
            _processor = processor;
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
                    Message = $"Operasi {step.Operation} untuk SELECT tidak didukung.",
                    TransactionId = _processor.ActiveTransactionId
                };

                IEnumerable<Row> result = op.GetRows();

                if (!op.usePreviousTable)
                {
                    storage.holdStorage = storage.lastResult;
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
                TransactionId = _processor.ActiveTransactionId
            };
        }

        private ExecutionResult HandleInsert(string query)
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
                TransactionId = _processor.ActiveTransactionId,
                TableName = tableName,
                AfterImage = afterImage,
                BeforeImage = null,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleUpdate(string query)
        {
            // ekstrak nama tabel dari query
            string tableName = ExtractTableNameFromQuery(query, "UPDATE");

            // untuk UPDATE, harus baca data lama terlebih dahulu (BeforeImage)
            var retrieval = new DataRetrieval(tableName, new[] { "*" });
            var beforeRows = _storageManager.ReadBlock(retrieval).ToList();

            var data = new Dictionary<string, object>
            {
                ["example_col"] = "value"
            };

            var write = new DataWrite(tableName, data);
            var affected = _storageManager.WriteBlock(write);

            // serialize before dan after image
            string? beforeImage = beforeRows.Any() ? SerializeRowData(beforeRows.First()) : null;
            string afterImage = SerializeData(data);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affected} row(s) ditulis/diperbarui melalui Storage Manager.",
                TransactionId = _processor.ActiveTransactionId,
                TableName = tableName,
                BeforeImage = beforeImage,
                AfterImage = afterImage,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleDelete(string query)
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
                TransactionId = _processor.ActiveTransactionId,
                TableName = tableName,
                BeforeImage = beforeImage,
                AfterImage = null,
                RowIdentifier = "UNKNOWN"
            };
        }

        private ExecutionResult HandleUnrecognized(string query)
        {
            return new ExecutionResult()
            {
                Query = query,
                Success = false,
                Message = "Tipe DML query tidak dikenali atau belum didukung.",
                TransactionId = _processor.ActiveTransactionId
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