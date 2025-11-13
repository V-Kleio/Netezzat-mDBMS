using System;
using System.Threading;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;
using Action = mDBMS.Common.Models.CCM.Action;
using Response = mDBMS.Common.Models.CCM.Response;

namespace mDBMS.CLI.Mocks
{
    public class MockConcurrencyControlManager : IConcurrencyControl
    {
        private int _transactionCounter = 1000;

        public int begin_transaction()
        {
            var id = Interlocked.Increment(ref _transactionCounter);
            Console.WriteLine($"[MOCK CCM]: BeginTransaction dipanggil. ID = {id}");
            return id;
        }

        public void log_object(Row @object, int transaction_id)
        {
            Console.WriteLine($"[MOCK CCM]: LogObject dipanggil untuk transaksi {transaction_id}.");
        }

        public Response validate_object(Row @object, int transaction_id, Action action)
        {
            Console.WriteLine($"[MOCK CCM]: ValidateObject dipanggil untuk aksi '{action.action}' pada transaksi {transaction_id}.");
            return new Response
            {
                allowed = true,
                transaction_id = transaction_id
            };
        }

        public void end_transaction(int transaction_id)
        {
            Console.WriteLine($"[MOCK CCM]: EndTransaction dipanggil. ID = {transaction_id}");
        }
    }
}
