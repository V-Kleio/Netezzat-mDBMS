using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class AbortTransactionHandler : IQueryHandler
    {
        private readonly IConcurrencyControlManager _concurrencyControlManager;

        public AbortTransactionHandler(IConcurrencyControlManager concurrencyControlManager)
        {
            _concurrencyControlManager = concurrencyControlManager;
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            if (transactionId == -1)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Tidak ada transaksi aktif yang bisa di-ABORT.",
                    TransactionId = null
                };
            }

            _concurrencyControlManager.EndTransaction(transactionId, false);
            
            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"Transaksi {transactionId} telah di-ABORT.",
                TransactionId = transactionId
            };
        }
    }
}