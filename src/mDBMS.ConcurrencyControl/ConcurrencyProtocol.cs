namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Enum untuk memilih protokol concurrency control yang digunakan
/// </summary>
public enum ConcurrencyProtocol
{
    /// <summary>
    /// Two-Phase Locking (2PL) Protocol dengan Deadlock Detection.
    /// Menggunakan shared dan exclusive locks dengan growing/shrinking phase.
    /// </summary>
    TwoPhaseLocking,

    /// <summary>
    /// Timestamp Ordering (TO) Protocol.
    /// Setiap transaksi mendapat timestamp unik, validasi berdasarkan RTS/WTS objek.
    /// Tidak ada locks, tidak ada deadlock, tapi lebih banyak abort.
    /// </summary>
    TimestampOrdering,

    /// <summary>
    /// Optimistic Concurrency Control (OCC) / Validation-based Protocol.
    /// Tiga fase: Read (tanpa lock), Validation (cek konflik), Write (commit/abort).
    /// Cocok untuk workload dengan konflik rendah.
    /// </summary>
    OptimisticValidation
}
