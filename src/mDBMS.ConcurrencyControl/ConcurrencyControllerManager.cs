using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;
using System;
using System.Threading;
using Action = mDBMS.Common.Models.Action;

namespace mDBMS.ConcurrencyControl
{
    public class ConcurrencyControlManager : IConcurrencyControl
    {
        private static int _txCounter = 0;

        public int begin_transaction()
        {
            // Tulis pesan debug ke konsol
            int id = Interlocked.Increment(ref _txCounter);
            Console.WriteLine("[MOCK CCM]: begin_transaction() dipanggil. Transaction Id={id}");
            // Kembalikan ID transaksi palsu
            return id;
        }

        public void log_object(Row @object, int transaction_id)
        {
            throw new NotImplementedException("log_object() belum diimplementasikan");
        }

        public Response validate_object(Row @object, int transaction_id, Action action)
        {
            // Cetak pesan debug
            Console.WriteLine($"[STUB CCM]: ValidateObject dipanggil");

            // Selalu izinkan
            return new Response
            {
                allowed = true,
                transaction_id = transaction_id
            };
        }

        public void end_transaction(int transaction_id)
        {
            throw new NotImplementedException("end_transaction() belum diimplementasikan");
        }
    }
}