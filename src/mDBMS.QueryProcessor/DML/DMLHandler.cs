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
            bool temporaryTransaction = transactionId == -1;

            if (temporaryTransaction)
            {
                transactionId = _concurrencyControlManager.BeginTransaction();
                _failureRecoveryManager.WriteLog(new()
                {
                    Operation = ExecutionLog.OperationType.BEGIN,
                    TransactionId = transactionId,
                    TableName = "",
                    RowIdentifier = "",
                });
            }

            string upper = query.Split()[0].Trim().ToUpperInvariant();
            ExecutionResult result = upper switch
            {
                "SELECT" => HandleSelect(query, transactionId),
                "INSERT" => HandleInsert(query, transactionId),
                "UPDATE" => HandleUpdate(query, transactionId),
                "DELETE" => HandleDelete(query, transactionId),
                _ => HandleUnrecognized(query, transactionId)
            };

            if (temporaryTransaction)
            {
                _concurrencyControlManager.CommitTransaction(transactionId);
                _failureRecoveryManager.WriteLog(new()
                {
                    Operation = ExecutionLog.OperationType.COMMIT,
                    TransactionId = transactionId,
                    TableName = "",
                    RowIdentifier = "",
                });
            }

            return result;
        }

        private ExecutionResult HandleSelect(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // Dapatkan Query Plan dari Optimizer
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);
            List<Row>? resultData;
            
            try
            {
                if (queryPlan.PlanTree is null)
                {
                    throw new Exception("Could not retrieve query plan");
                }

                resultData = queryPlan.PlanTree.AcceptVisitor(new Operator(
                    _storageManager,
                    _failureRecoveryManager,
                    _concurrencyControlManager,
                    transactionId
                )).ToList();

                // Validasi Read ke CCM
                foreach (Row row in resultData)
                {
                    string[] ids = row.id.Split(';');

                    foreach (string id in ids)
                    {
                        var action = new Common.Transaction.Action(
                            Common.Transaction.Action.ActionType.Read,
                            DatabaseObject.CreateRow(id, parsedQuery.Table),
                            transactionId,
                            query
                        );
            
                        var response = _concurrencyControlManager.ValidateObject(action);
            
                        if (!response.Allowed)
                        {
                            throw new Exception($"Read operation ditolak oleh CCM: {response.Reason}");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (transactionId != -1)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);

                    _failureRecoveryManager.WriteLog(new()
                    {
                        Operation = ExecutionLog.OperationType.COMMIT,
                        TransactionId = transactionId,
                        TableName = "",
                        RowIdentifier = "",
                    });

                    _failureRecoveryManager.UndoTransaction(transactionId);
                }

                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = e.Message,
                    Data = null,
                    TransactionId = -1,
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
        
        private ExecutionResult HandleInsert(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // Dapatkan Query Plan dari Optimizer
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);
            List<Row>? resultData;
            
            try
            {
                if (queryPlan.PlanTree is null)
                {
                    throw new Exception("Could not retrieve query plan");
                }

                resultData = queryPlan.PlanTree.AcceptVisitor(new Operator(
                    _storageManager,
                    _failureRecoveryManager,
                    _concurrencyControlManager,
                    transactionId
                )).ToList();
            }
            catch (Exception e)
            {
                if (transactionId != -1)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);

                    _failureRecoveryManager.WriteLog(new()
                    {
                        Operation = ExecutionLog.OperationType.ABORT,
                        TransactionId = transactionId,
                        TableName = "",
                        RowIdentifier = "",
                    });

                    _failureRecoveryManager.UndoTransaction(transactionId);
                }

                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = e.Message,
                    Data = null,
                    TransactionId = -1,
                };
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = "Row baru telah dimuatkan.",
                Data = null,
                TransactionId = transactionId
            };
        }

        private ExecutionResult HandleUpdate(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // Dapatkan Query Plan dari Optimizer
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);
            List<Row>? resultData;
            
            try
            {
                if (queryPlan.PlanTree is null)
                {
                    throw new Exception("Could not retrieve query plan");
                }

                resultData = queryPlan.PlanTree.AcceptVisitor(new Operator(
                    _storageManager,
                    _failureRecoveryManager,
                    _concurrencyControlManager,
                    transactionId
                )).ToList();
            }
            catch (Exception e)
            {
                if (transactionId != -1)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);

                    _failureRecoveryManager.WriteLog(new()
                    {
                        Operation = ExecutionLog.OperationType.ABORT,
                        TransactionId = transactionId,
                        TableName = "",
                        RowIdentifier = "",
                    });

                    _failureRecoveryManager.UndoTransaction(transactionId);
                }

                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = e.Message,
                    Data = null,
                    TransactionId = -1,
                };
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = "Row telah diperbarui.",
                Data = null,
                TransactionId = transactionId
            };
        }

        private ExecutionResult HandleDelete(string query, int transactionId)
        {
            Query parsedQuery = _queryOptimizer.ParseQuery(query);

            // Dapatkan Query Plan dari Optimizer
            QueryPlan queryPlan = _queryOptimizer.OptimizeQuery(parsedQuery);
            List<Row>? resultData;
            
            try
            {
                if (queryPlan.PlanTree is null)
                {
                    throw new Exception("Could not retrieve query plan");
                }

                resultData = queryPlan.PlanTree.AcceptVisitor(new Operator(
                    _storageManager,
                    _failureRecoveryManager,
                    _concurrencyControlManager,
                    transactionId
                )).ToList();
            }
            catch (Exception e)
            {
                if (transactionId != -1)
                {
                    _concurrencyControlManager.AbortTransaction(transactionId);

                    _failureRecoveryManager.WriteLog(new()
                    {
                        Operation = ExecutionLog.OperationType.ABORT,
                        TransactionId = transactionId,
                        TableName = "",
                        RowIdentifier = "",
                    });

                    _failureRecoveryManager.UndoTransaction(transactionId);
                }

                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = e.Message,
                    Data = null,
                    TransactionId = -1,
                };
            }

            return new ExecutionResult()
            {
                Query = query,
                Success = true,

                Message = "Row telah dihapus.",
                Data = null,
                TransactionId = transactionId
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
    }
}
