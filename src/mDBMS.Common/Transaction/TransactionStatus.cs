namespace mDBMS.Common.Transaction;

/// <summary>
/// Enum yang mendefinisikan status transaksi
/// </summary>
public enum TransactionStatus
{
    /// <summary>
    /// Transaksi sedang aktif
    /// </summary>
    Active,

    /// <summary>
    /// Transaksi telah menyelesaikan eksekusi terakhir tetapi belum commit
    /// </summary>
    PartiallyCommitted,

    /// <summary>
    /// Transaksi telah berhasil di-commit
    /// </summary>
    Committed,

    /// <summary>
    /// Transaksi gagal dan harus di-abort
    /// </summary>
    Failed,

    /// <summary>
    /// Transaksi telah di-abort
    /// </summary>
    Aborted,

    /// <summary>
    /// Transaksi telah selesai (baik committed maupun aborted) dan sumber daya telah dibebaskan
    /// </summary>
    Terminated,

    /// <summary>
    /// Transaksi dalam keadaan waiting (menunggu lock) - Internal State
    /// </summary>
    Waiting
}
