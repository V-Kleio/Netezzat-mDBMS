namespace mDBMS.Common.Models.CCM;

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
    /// Transaksi sedang dalam proses commit
    /// </summary>
    Committing,

    /// <summary>
    /// Transaksi telah berhasil di-commit
    /// </summary>
    Committed,

    /// <summary>
    /// Transaksi sedang dalam proses abort
    /// </summary>
    Aborting,

    /// <summary>
    /// Transaksi telah di-abort
    /// </summary>
    Aborted,

    /// <summary>
    /// Transaksi dalam keadaan waiting (menunggu lock)
    /// </summary>
    Waiting,

    /// <summary>
    /// Transaksi mengalami deadlock
    /// </summary>
    Deadlocked
}
