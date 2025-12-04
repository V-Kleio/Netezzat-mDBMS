using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class BeginTransactionHandler : IQueryHandler
    {
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager; 

        public BeginTransactionHandler(IConcurrencyControlManager concurrencyControlManager, IFailureRecoveryManager failureRecoveryManager) 
        {
            _concurrencyControlManager = concurrencyControlManager;
            _failureRecoveryManager = failureRecoveryManager; 
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            if (transactionId != -1)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Transaksi sudah aktif dengan ID {transactionId}.",
                    TransactionId = transactionId
                };
            }

            transactionId = _concurrencyControlManager.BeginTransaction();
            _failureRecoveryManager.WriteLog(new()
            {
                Operation = ExecutionLog.OperationType.BEGIN,
                TransactionId = transactionId,
                TableName = "",
                RowIdentifier = "",
            });

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"Transaksi baru dimulai dengan ID {transactionId}.",
                TransactionId = transactionId
            };
        }
    }
}