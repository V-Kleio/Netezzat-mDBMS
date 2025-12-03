using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class CommitTransactionHandler : IQueryHandler
    {
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager; 

        public CommitTransactionHandler(IConcurrencyControlManager concurrencyControlManager, IFailureRecoveryManager failureRecoveryManager) 
        {
            _concurrencyControlManager = concurrencyControlManager;
            _failureRecoveryManager = failureRecoveryManager; 
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            if (transactionId == -1) 
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Tidak ada transaksi aktif yang bisa di-COMMIT.",
                    TransactionId = -1
                };
            }

            // 1. Panggil CCM untuk me-release lock dan ganti status
            _concurrencyControlManager.EndTransaction(transactionId, true);

            // Kita berasumsi WriteLog untuk COMMIT sudah dipanggil di QueryProcessor.cs.

            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"Transaksi {transactionId} berhasil di-COMMIT.",
                TransactionId = transactionId
            };
        }
    }
}