namespace mDBMS.Common.Transaction;

/// <summary>
/// DTO yang merepresentasikan respons dari Concurrency Control Manager.
/// Dikirim kembali ke Query Processor sebagai hasil validasi.
/// </summary>
public class Response
{
    /// <summary>
    /// Aksi diizinkan (true) atau tidak (false).
    /// </summary>
    public bool Allowed { get; set; }

    /// <summary>
    /// ID Transaksi terkait.
    /// </summary>
    public int TransactionId { get; set; }

    /// <summary>
    /// Objek database yang divalidasi
    /// </summary>
    public DatabaseObject? DatabaseObject { get; set; }

    /// <summary>
    /// Tipe aksi yang divalidasi
    /// </summary>
    public Action.ActionType ActionType { get; set; }

    /// <summary>
    /// Timestamp ketika respons diberikan
    /// </summary>
    public DateTime RespondedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Alasan jika aksi tidak diizinkan
    /// Contoh: "Conflicting transaction", "Deadlock detected", dll.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// Status kode untuk respons
    /// </summary>
    public ResponseStatus Status { get; set; }

    /// <summary>
    /// Konstruktor default
    /// </summary>
    public Response()
    {
    }

    /// <summary>
    /// Konstruktor dengan parameter
    /// </summary>
    public Response(bool allowed, int transactionId, ResponseStatus status, string? reason = null)
    {
        Allowed = allowed;
        TransactionId = transactionId;
        Status = status;
        Reason = reason;
    }

    /// <summary>
    /// Factory method untuk membuat respons yang diizinkan
    /// </summary>
    public static Response CreateAllowed(int transactionId, DatabaseObject? obj = null, Action.ActionType actionType = Action.ActionType.Read)
    {
        return new Response
        {
            Allowed = true,
            TransactionId = transactionId,
            DatabaseObject = obj,
            ActionType = actionType,
            Status = ResponseStatus.Granted,
            Reason = "Action permitted"
        };
    }

    /// <summary>
    /// Factory method untuk membuat respons yang ditolak
    /// </summary>
    public static Response CreateDenied(int transactionId, string reason, DatabaseObject? obj = null, Action.ActionType actionType = Action.ActionType.Read)
    {
        return new Response
        {
            Allowed = false,
            TransactionId = transactionId,
            DatabaseObject = obj,
            ActionType = actionType,
            Status = ResponseStatus.Denied,
            Reason = reason
        };
    }

    /// <summary>
    /// Factory method untuk membuat respons dengan status waiting
    /// </summary>
    public static Response CreateWaiting(int transactionId, string reason, DatabaseObject? obj = null, Action.ActionType actionType = Action.ActionType.Read)
    {
        return new Response
        {
            Allowed = false,
            TransactionId = transactionId,
            DatabaseObject = obj,
            ActionType = actionType,
            Status = ResponseStatus.Waiting,
            Reason = reason
        };
    }

    /// <summary>
    /// Untuk debugging
    /// </summary>
    public override string ToString()
    {
        return $"Response[TXN-{TransactionId}]: {Status} - {(Allowed ? "ALLOWED" : "DENIED")} ({Reason})";
    }

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
}