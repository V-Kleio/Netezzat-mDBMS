namespace mDBMS.Common.Models.CCM;

/// <summary>
/// Enum yang mendefinisikan status respons dari CCM
/// </summary>
public enum ResponseStatus
{
    /// <summary>
    /// Aksi diizinkan dan lock telah diberikan
    /// </summary>
    Granted,

    /// <summary>
    /// Aksi ditolak karena konflik dengan transaksi lain
    /// </summary>
    Denied,

    /// <summary>
    /// Transaksi harus menunggu karena objek sedang digunakan
    /// </summary>
    Waiting,

    /// <summary>
    /// Deadlock terdeteksi
    /// </summary>
    Deadlock,

    /// <summary>
    /// Transaksi telah di-abort
    /// </summary>
    Aborted,

    /// <summary>
    /// Error internal dalam CCM
    /// </summary>
    Error
}
