namespace mDBMS.Common.Transaction;

/// <summary>
/// DTO yang merepresentasikan sebuah aksi yang akan dilakukan pada objek database.
/// </summary>
public class Action
{
    /// <summary>
    /// Tipe aksi yang akan dilakukan (Read atau Write)
    /// </summary>
    public ActionType Type { get; set; }

    /// <summary>
    /// Objek database yang akan diakses
    /// </summary>
    public DatabaseObject DatabaseObject { get; set; } = new();

    /// <summary>
    /// ID transaksi yang melakukan aksi
    /// </summary>
    public int TransactionId { get; set; }

    /// <summary>
    /// Timestamp ketika aksi diminta
    /// </summary>
    public DateTime RequestedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Informasi tambahan
    /// Contoh: "SELECT * FROM table", "UPDATE table SET ...", dll.
    /// </summary>
    public string? AdditionalInfo { get; set; }

    /// <summary>
    /// Konstruktor default
    /// </summary>
    public Action()
    {
    }

    /// <summary>
    /// Konstruktor dengan parameter
    /// </summary>
    public Action(ActionType actionType, DatabaseObject databaseObject, int transactionId, string? additionalInfo = null)
    {
        Type = actionType;
        DatabaseObject = databaseObject ?? throw new ArgumentNullException(nameof(databaseObject));
        TransactionId = transactionId;
        AdditionalInfo = additionalInfo;
    }

    /// <summary>
    /// Factory method untuk membuat aksi Read
    /// </summary>
    public static Action CreateReadAction(DatabaseObject obj, int transactionId, string? info = null)
    {
        return new Action(ActionType.Read, obj, transactionId, info);
    }

    /// <summary>
    /// Factory method untuk membuat aksi Write
    /// </summary>
    public static Action CreateWriteAction(DatabaseObject obj, int transactionId, string? info = null)
    {
        return new Action(ActionType.Write, obj, transactionId, info);
    }

    /// <summary>
    /// Untuk debugging
    /// </summary>
    // public override string ToString()
    // {
    //     return $"Action[TXN-{TransactionId}]: {ActionType} on {DatabaseObject}";
    // }

    /// <summary>
    /// Enum yang mendefinisikan tipe aksi yang dapat dilakukan pada objek database.
    /// </summary>
    public enum ActionType
    {
        /// <summary>
        /// Operasi (SELECT, Read)
        /// </summary>
        Read,
    
        /// <summary>
        /// Operasi (INSERT, UPDATE, DELETE)
        /// </summary>
        Write
    }
}