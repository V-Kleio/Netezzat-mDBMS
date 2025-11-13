using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;
using System;

namespace mDBMS.CLI
{
    /// <summary>
    /// Ini adalah implementasi MOCK (palsu) dari IConcurrencyControl.
    /// Tujuannya HANYA untuk menguji Query Processor dan CLI,
    /// tanpa harus menunggu Grup CCM selesai.
    /// Sesuai Panduan Tugas Besar, Fase 1, Tugas 5.1 untuk Grup QP.
    /// </summary>
    public class MockCCM : IConcurrencyControl
    {
        public int begin_transaction()
        {
            // Tulis pesan debug ke konsol
            Console.WriteLine("[MOCK CCM]: begin_transaction() dipanggil.");
            
            // Kembalikan ID transaksi palsu
            return 12345;
        }

        public void log_object(Row @object, int transaction_id)
        {
            // Tulis pesan debug ke konsol
            Console.WriteLine($"[MOCK CCM]: log_object() dipanggil untuk Tx: {transaction_id}.");
        }

        public Response validate_object(Row @object, int transaction_id, mDBMS.Common.Models.CCM.Action action)
        {
            // Tulis pesan debug ke konsol
            Console.WriteLine($"[MOCK CCM]: validate_object() dipanggil untuk Tx: {transaction_id}, Aksi: {action.action}");

            // PENTING: Sesuai panduan Fase 1, stub ini harus SELALU MENGIZINKAN.
            // Ini agar pengujian Grup QP dan SM tidak terhambat.
            return new Response
            {
                allowed = true,
                transaction_id = transaction_id
            };
        }

        public void end_transaction(int transaction_id)
        {
            // Tulis pesan debug ke konsol
            Console.WriteLine($"[MOCK CCM]: end_transaction() dipanggil untuk Tx: {transaction_id}.");
        }
    }
}