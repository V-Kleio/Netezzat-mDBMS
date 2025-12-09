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
            Console.WriteLine($"[INFO] Operasi Begin diterima");

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
            LogEntry logEntry = LogEntry.CreateBeginTransaction(0, transactionId);
            _failureRecoveryManager.WriteLogEntry(logEntry);

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