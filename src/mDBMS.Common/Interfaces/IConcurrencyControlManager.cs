using mDBMS.Common.Transaction;

using Response = mDBMS.Common.Transaction.Response;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.Common.Interfaces;

/// <summary>
/// Interface Concurrency Control Manager (CCM).
/// </summary>
public interface IConcurrencyControlManager
{
    /// <summary>
    /// Memulai transaksi baru dan memberikan transaction ID unik.
    /// </summary>
    /// <returns>Transaction ID yang baru dibuat</returns>
    int BeginTransaction();

    /// <summary>
    /// Memvalidasi apakah suatu aksi pada objek database diizinkan untuk transaksi tertentu.
    /// </summary>
    /// <param name="action">Objek Action yang berisi informasi tentang aksi yang akan dilakukan</param>
    /// <returns>Response yang menunjukkan apakah aksi diizinkan atau tidak</returns>
    Response ValidateObject(Action action);

    /// <summary>
    /// Mengakhiri transaksi dengan commit atau abort.
    /// Semua locks yang dipegang oleh transaksi akan dilepaskan.
    /// </summary>
    /// <param name="transactionId">ID transaksi yang akan diakhiri</param>
    /// <param name="commit">True untuk commit, false untuk abort</param>
    /// <returns>True jika transaksi berhasil diakhiri, false jika gagal</returns>
    bool EndTransaction(int transactionId, bool commit);

    /// <summary>
    /// Mendapatkan status transaksi saat ini.
    /// </summary>
    /// <param name="transactionId">ID transaksi</param>
    /// <returns>Status transaksi</returns>
    TransactionStatus GetTransactionStatus(int transactionId);

    /// <summary>
    /// Apakah transaksi masih aktif.
    /// </summary>
    /// <param name="transactionId">ID transaksi</param>
    /// <returns>True jika transaksi aktif, false jika tidak</returns>
    bool IsTransactionActive(int transactionId);

    /// <summary>
    /// Abort transaksi.
    /// </summary>
    /// <param name="transactionId">ID transaksi yang akan di-abort</param>
    /// <returns>True jika berhasil, false jika gagal</returns>
    bool AbortTransaction(int transactionId);

    /// <summary>
    /// Commit transaksi.
    /// </summary>
    /// <param name="transactionId">ID transaksi yang akan di-commit</param>
    /// <returns>True jika berhasil, false jika gagal</returns>
    bool CommitTransaction(int transactionId);
}
