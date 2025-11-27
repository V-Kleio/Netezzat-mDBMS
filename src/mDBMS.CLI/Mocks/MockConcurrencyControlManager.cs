using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

using Action = mDBMS.Common.Transaction.Action;
using Response = mDBMS.Common.Transaction.Response;

namespace mDBMS.CLI.Mocks
{
    public class MockConcurrencyControlManager : IConcurrencyControlManager
    {
        private int _transactionCounter = 1000;

        public int BeginTransaction()
        {
            var id = Interlocked.Increment(ref _transactionCounter);
            Console.WriteLine($"[MOCK CCM]: BeginTransaction dipanggil. ID = {id}");
            return id;
        }

        public Response ValidateObject(Action action)
        {
            Console.WriteLine($"[MOCK CCM]: ValidateObject dipanggil untuk aksi '{action.Type}' pada transaksi {action.TransactionId}.");
            return new Response
            {
                Allowed = true,
                TransactionId = action.TransactionId
            };
        }

        public bool EndTransaction(int transaction_id, bool commit)
        {
            Console.WriteLine($"[MOCK CCM]: EndTransaction dipanggil. ID = {transaction_id}");
            return commit;
        }

        public TransactionStatus GetTransactionStatus(int transactionId)
        {
            return TransactionStatus.Active;
        }

        public bool IsTransactionActive(int transactionId)
        {
            return true;
        }

        public bool AbortTransaction(int transactionId)
        {
            Console.WriteLine($"[MOCK CCM]: AbortTransaction dipanggil. ID = {transactionId}");
            return true;
        }

        public bool CommitTransaction(int transactionId)
        {
            Console.WriteLine($"[MOCK CCM]: CommitTransaction dipanggil. ID = {transactionId}");
            return true;
        }

        public void LogObject(DatabaseObject obj, int transactionId)
        {
            Console.WriteLine($"[MOCK CCM]: LogObject dipanggil. T{transactionId} mengakses {obj.ToQualifiedString()}");
        }
    }
}
