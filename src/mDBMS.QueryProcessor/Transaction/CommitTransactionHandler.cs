using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class CommitTransactionHandler : IQueryHandler
    {
        private readonly IConcurrencyControlManager _concurrencyControlManager;

        public CommitTransactionHandler(IConcurrencyControlManager concurrencyControlManager)
        {
            _concurrencyControlManager = concurrencyControlManager;
        }

        public ExecutionResult HandleQuery(string query, int transactionId)
        {
            if (transactionId != -1)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Tidak ada transaksi aktif yang bisa di-COMMIT.",
                    TransactionId = null
                };
            }

            _concurrencyControlManager.EndTransaction(transactionId, true);

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