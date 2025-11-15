using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.Transaction
{
    internal class BeginTransactionHandler : IQueryHandler
    {
        private readonly QueryProcessor _processor;
        private readonly IConcurrencyControlManager _concurrencyControlManager;

        public BeginTransactionHandler(QueryProcessor processor, IConcurrencyControlManager concurrencyControlManager)
        {
            _processor = processor;
            _concurrencyControlManager = concurrencyControlManager;
        }

        public ExecutionResult HandleQuery(string query)
        {
            if (_processor.ActiveTransactionId.HasValue)
            {
                return new ExecutionResult()
                {
                    Query = query,
                    Success = false,
                    Message = $"Transaksi sudah aktif dengan ID {_processor.ActiveTransactionId.Value}."
                };
            }

            _processor.ActiveTransactionId = _concurrencyControlManager.BeginTransaction();
            return new ExecutionResult()
            {
                Query = query,
                Success = true,
                Message = $"Transaksi baru dimulai dengan ID {_processor.ActiveTransactionId.Value}."
            };
        }
    }
}