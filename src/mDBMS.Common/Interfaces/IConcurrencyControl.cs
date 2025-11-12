// Kita perlu 'using' Models yang sudah ada dan yang baru kita buat
using mDBMS.Common.Models;

using Response = mDBMS.Common.Models.CCM.Response;
using Action = mDBMS.Common.Models.CCM.Action;

namespace mDBMS.Common.Interfaces
{
    /// <summary>
    /// Kontrak untuk Concurrency Control Manager (CCM).
    /// Mendefinisikan method-method sesuai spesifikasi halaman 4.
    /// </summary>
    public interface IConcurrencyControl
    {
        /// <summary>
        /// Memberikan transaction id pada transaksi yang baru dimulai.
        /// </summary>
        /// <returns>Transaction ID (int)</returns>
        int begin_transaction();

        /// <summary>
        /// Mencatat (log) sebuah objek pada transaksi tertentu.
        /// </summary>
        /// <param name="object">Objek 'Row' yang akan dicatat</param>
        /// <param name="transaction_id">ID transaksi</param>
        void log_object(Row @object, int transaction_id);

        /// <summary>
        /// Memvalidasi apakah suatu object diizinkan melakukan aksi tertentu.
        /// </summary>
        /// <param name="object">Objek 'Row' yang akan divalidasi</param>
        /// <param name="transaction_id">ID transaksi</param>
        /// <param name="action">DTO 'Action' yang berisi tipe aksi</param>
        /// <returns>DTO 'Response' yang berisi status izin</returns>
        Response validate_object(Row @object, int transaction_id, Action action);

        /// <summary>
        /// Mengakhiri transaksi (commit atau abort).
        /// </summary>
        /// <param name="transaction_id">ID transaksi yang akan diakhiri</param>
        void end_transaction(int transaction_id);
    }
}