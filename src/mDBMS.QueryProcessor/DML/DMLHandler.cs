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

            // Validasi Read permission ke CCM
            var action = new Common.Transaction.Action(
                Common.Transaction.Action.ActionType.Read,
                DatabaseObject.CreateRow("ANY", parsedQuery.Table),
                transactionId,
                query
            );

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

            // Dapatkan Query Plan dari Optimizer
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);
            IEnumerable<Row> resultData = Enumerable.Empty<Row>();

            try 
            {
                if (queryPlan.PlanTree != null)
                {
                    // Eksekusi menggunakan PlanTree (Recursive/Pipeline)
                    resultData = ExecuteNode(queryPlan.PlanTree);
                }
                else
                {
                    // Fallback ke linear steps jika PlanTree null
                    LocalTableStorage storage = new LocalTableStorage();
                    foreach (QueryPlanStep step in queryPlan.Steps)
                    {
                        Operator? op = CreateOperator(step, storage);
                        if (op == null) throw new Exception($"Operasi {step.Operation} tidak didukung.");

                        IEnumerable<Row> result = op.GetRows();
                        
                        // Update pipeline storage
                        LocalTableStorage oldStorage = storage;
                        storage = new LocalTableStorage();
                        if (!op.usePreviousTable) storage.holdStorage = oldStorage.lastResult;
                        storage.lastResult = result;
                    }
                    resultData = storage.lastResult;
                }
            }
            catch (Exception ex)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Error saat eksekusi: {ex.Message}",
                    TransactionId = transactionId
                };
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = "Query berhasil dieksekusi.",
                Data = resultData,
                TransactionId = transactionId
            };
        }

        // Eksekusi node secara rekursif (Pipeline)
        private IEnumerable<Row> ExecuteNode(PlanNode node)
        {
            IEnumerable<Row>? inputRows = null;
            IEnumerable<Row>? leftRows = null;
            IEnumerable<Row>? rightRows = null;

            // Rekursif ke child nodes terlebih dahulu
            if (node is FilterNode fn) inputRows = ExecuteNode(fn.Input);
            else if (node is ProjectNode pn) inputRows = ExecuteNode(pn.Input);
            else if (node is SortNode sn) inputRows = ExecuteNode(sn.Input);
            else if (node is AggregateNode an) inputRows = ExecuteNode(an.Input);
            else if (node is JoinNode jn)
            {
                leftRows = ExecuteNode(jn.Left);
                rightRows = ExecuteNode(jn.Right);
            }

            // Siapkan storage sementara untuk operator
            var tempStorage = new LocalTableStorage();
            
            if (inputRows != null) 
            {
                tempStorage.lastResult = inputRows; 
            }
            else if (leftRows != null && rightRows != null)
            {
                // Masukkan hasil join ke storage (Left->Hold, Right->Last)
                tempStorage.holdStorage = leftRows;
                tempStorage.lastResult = rightRows;
            }

            // Konversi Node ke Step agar kompatibel dengan Operator lama
            QueryPlanStep step = CreateStepFromNode(node);

            // Jalankan Operator
            Operator? op = CreateOperator(step, tempStorage);
            
            if (op == null) throw new InvalidOperationException($"Operator untuk {node.GetType().Name} tidak ditemukan.");

            return op.GetRows();
        }

        // Factory untuk membuat instance Operator
        private Operator? CreateOperator(QueryPlanStep step, LocalTableStorage storage)
        {
            return step.Operation switch
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
        }

        // Mapping dari PlanNode (Tree) ke QueryPlanStep (Flat)
        private QueryPlanStep CreateStepFromNode(PlanNode node)
        {
            var step = new QueryPlanStep
            {
                EstimatedCost = node.NodeCost,
                Description = node.Details
            };

            switch (node)
            {
                case TableScanNode tsn:
                    step.Operation = OperationType.TABLE_SCAN;
                    step.Table = tsn.TableName;
                    step.Parameters["table"] = tsn.TableName;
                    break;

                case IndexScanNode isn:
                    step.Operation = OperationType.INDEX_SCAN;
                    step.Table = isn.TableName;
                    step.IndexUsed = isn.IndexColumn;
                    step.Parameters["table"] = isn.TableName;
                    step.Parameters["indexColumn"] = isn.IndexColumn;
                    break;

                case IndexSeekNode isn:
                    step.Operation = OperationType.INDEX_SEEK;
                    step.Table = isn.TableName;
                    step.IndexUsed = isn.IndexColumn;
                    step.Parameters["table"] = isn.TableName;
                    step.Parameters["indexColumn"] = isn.IndexColumn;
                    if (isn.SeekCondition.Any()) step.Parameters["condition"] = isn.SeekCondition.First();
                    break;

                case FilterNode fn:
                    step.Operation = OperationType.FILTER;
                    if (fn.Conditions.Any()) step.Parameters["Condition"] = fn.Conditions.First();
                    break;

                case ProjectNode pn:
                    step.Operation = OperationType.PROJECTION;
                    step.Parameters["columns"] = pn.Columns;
                    break;

                case SortNode sn:
                    step.Operation = OperationType.SORT;
                    step.Parameters["orderBy"] = sn.OrderBy.Select(o => new Dictionary<string, object?> {
                        { "column", o.Column },
                        { "ascending", o.IsAscending }
                    }).ToList();
                    break;

                case JoinNode jn:
                    step.Operation = jn.Algorithm switch {
                        JoinAlgorithm.Hash => OperationType.HASH_JOIN,
                        JoinAlgorithm.Merge => OperationType.MERGE_JOIN,
                        _ => OperationType.NESTED_LOOP_JOIN
                    };
                    step.Parameters["on"] = $"{jn.JoinCondition.lhs}={jn.JoinCondition.rhs}";
                    step.Table = ""; // Kosongkan table agar pakai hasil previous
                    break;
            }

            return step;
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

            DataRetrieval readRequest = new DataRetrieval(parsedQuery.Table, [], [[condition]]);
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

            DataWrite writeRequest = new(parsedQuery.Table, newValues, [[condition]]);
            int affectedRowCount = _storageManager.WriteBlock(writeRequest);
            var afterImage = SerializeData(_storageManager.ReadBlock(readRequest).ToList().First().Columns);

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{affectedRowCount} row(s) ditulis/diperbarui melalui Storage Manager.",
                TransactionId = transactionId,
            };
        }

        private ExecutionResult HandleDelete(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            Condition? condition = ParseCondition(parsedQuery.WhereClause);
            if (!string.IsNullOrWhiteSpace(parsedQuery.WhereClause) && condition == null)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Format WHERE untuk DELETE tidak dikenali.",
                    TransactionId = transactionId
                };
            }

            var conditions = BuildConditionGroups(condition);
            var retrieval = new DataRetrieval(parsedQuery.Table, new[] { "*" }, conditions);
            var targetRows = _storageManager
                .ReadBlock(retrieval)
                .Select(row => (Row: row, Identifier: BuildRowIdentifier(row)))
                .ToList();

            if (targetRows.Count == 0)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = true,
                    Message = "0 row(s) cocok untuk dihapus.",
                    TransactionId = transactionId
                };
            }

            foreach (var (row, identifier) in targetRows)
            {
                var action = new Common.Transaction.Action(
                    Common.Transaction.Action.ActionType.Write,
                    DatabaseObject.CreateRow(identifier, parsedQuery.Table),
                    transactionId,
                    query
                );

                var response = _concurrencyControlManager.ValidateObject(action);
                if (!response.Allowed)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);

                    return new ExecutionResult()
                    {
                        Query = query,
                        Success = false,
                        Message = $"DELETE ditolak oleh CCM untuk row {identifier}: {response.Reason}",
                        TransactionId = transactionId
                    };
                }
            }

            int deletedCount = _storageManager.DeleteBlock(new DataDeletion(parsedQuery.Table, conditions));

            foreach (var (row, identifier) in targetRows)
            {
                _failureRecoveryManager.WriteLog(new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.DELETE,
                    TransactionId = transactionId,
                    TableName = parsedQuery.Table,
                    RowIdentifier = identifier,
                    BeforeImage = row,
                    AfterImage = null
                });
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"{deletedCount} row(s) dihapus melalui Storage Manager.",
                TransactionId = transactionId,
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

        private static Condition? ParseCondition(string? whereClause)
        {
            if (string.IsNullOrWhiteSpace(whereClause))
            {
                return null;
            }

            var operations = new (string token, Condition.Operation op)[]
            {
                (">=", Condition.Operation.GEQ),
                ("<=", Condition.Operation.LEQ),
                ("<>", Condition.Operation.NEQ),
                ("!=", Condition.Operation.NEQ),
                (">", Condition.Operation.GT),
                ("<", Condition.Operation.LT),
                ("=", Condition.Operation.EQ),
            };

            foreach (var (token, op) in operations)
            {
                int idx = whereClause.IndexOf(token, StringComparison.OrdinalIgnoreCase);
                if (idx <= 0) continue;

                string lhs = StripTableName(whereClause[..idx].Trim());
                string rhs = NormalizeLiteral(whereClause[(idx + token.Length)..].Trim());

                return new Condition
                {
                    lhs = lhs,
                    rhs = rhs,
                    opr = op,
                    rel = Condition.Relation.COLUMN_AND_VALUE
                };
            }

            return null;
        }

        private static IEnumerable<IEnumerable<Condition>>? BuildConditionGroups(Condition? condition)
        {
            if (condition == null)
            {
                return null;
            }

            return new List<IEnumerable<Condition>>
            {
                new List<Condition> { condition }
            };
        }

        private string BuildRowIdentifier(Row row)
        {
            string? pkColumn = row.Columns.Keys.FirstOrDefault(key => key.Equals("id", StringComparison.OrdinalIgnoreCase))
                ?? row.Columns.Keys.FirstOrDefault(key => key.EndsWith("id", StringComparison.OrdinalIgnoreCase))
                ?? row.Columns.Keys.FirstOrDefault();

            object? pkValue = null;
            if (pkColumn != null)
            {
                row.Columns.TryGetValue(pkColumn, out pkValue);
            }

            string identifier = pkColumn != null
                ? $"{StripTableName(pkColumn)}={FormatIdentifierValue(pkValue)}"
                : $"ROW-{Guid.NewGuid()}";

            if (string.IsNullOrEmpty(row.id))
            {
                row.id = identifier;
            }

            return identifier;
        }

        private static string StripTableName(string column)
        {
            if (string.IsNullOrWhiteSpace(column))
            {
                return column;
            }

            var parts = column.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return parts.Length == 0 ? column : parts[^1];
        }

        private static string NormalizeLiteral(string raw)
        {
            string trimmed = raw.Trim().TrimEnd(';');
            return trimmed.Trim('\'', '"');
        }

        private static string FormatIdentifierValue(object? value)
        {
            if (value == null)
            {
                return "NULL";
            }

            string text = value.ToString() ?? "NULL";

            if (double.TryParse(text, out _))
            {
                return text;
            }

            return $"'{text.Replace("'", "''")}'";
        }
    }
}
