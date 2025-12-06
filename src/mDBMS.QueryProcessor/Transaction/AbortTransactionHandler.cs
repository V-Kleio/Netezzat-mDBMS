using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class AbortTransactionHandler : IQueryHandler
    {
        private readonly IConcurrencyControlManager _concurrencyControlManager;
        private readonly IFailureRecoveryManager _failureRecoveryManager; 

        public AbortTransactionHandler(IConcurrencyControlManager concurrencyControlManager, IFailureRecoveryManager failureRecoveryManager) 
        {
            _concurrencyControlManager = concurrencyControlManager;
            _failureRecoveryManager = failureRecoveryManager; 
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            Console.WriteLine($"[INFO] Operasi Abort diterima");

            if (transactionId == -1) // Cek apakah ada transaksi aktif
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Tidak ada transaksi aktif yang bisa di-ABORT.",
                    TransactionId = -1
                };
            }

            // 1. Panggil CCM untuk me-release lock dan ganti status
            _concurrencyControlManager.EndTransaction(transactionId, false);
            
            // 2. Panggil FRM untuk melakukan UNDO Recovery
            bool undoSuccess = _failureRecoveryManager.UndoTransaction(transactionId);
            
            return new ExecutionResult()
            {
                Query = query,
                Success = undoSuccess,
                Message = undoSuccess 
                    ? $"Transaksi {transactionId} telah di-ABORT dan UNDO berhasil." 
                    : $"Transaksi {transactionId} di-ABORT, namun UNDO gagal.",
                TransactionId = transactionId,
            };
        }
    }
}