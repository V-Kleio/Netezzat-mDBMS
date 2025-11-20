using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class AbortTransactionHandler : IQueryHandler
    {
        private readonly QueryProcessor _processor;
        private readonly IConcurrencyControlManager _concurrencyControlManager;

        public AbortTransactionHandler(QueryProcessor processor, IConcurrencyControlManager concurrencyControlManager)
        {
            _processor = processor;
            _concurrencyControlManager = concurrencyControlManager;
        }

        public ExecutionResult HandleQuery(string query)
        {
            if (!_processor.ActiveTransactionId.HasValue)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = "Tidak ada transaksi aktif yang bisa di-ABORT."
                };
            }

            var transactionId = _processor.ActiveTransactionId.Value;
            _concurrencyControlManager.EndTransaction(transactionId, false);
            _processor.ActiveTransactionId = null;
            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"Transaksi {transactionId} telah di-ABORT."
            };
        }
    }
}