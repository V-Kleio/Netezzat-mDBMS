using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Facade/Factory untuk Concurrency Control Manager yang mendukung multiple protocols.
/// Delegates semua operasi ke protocol manager yang dipilih.
/// </summary>
public class ConcurrencyControlManager : IConcurrencyControlManager
{
    private readonly IConcurrencyControlManager _protocolManager;
    private readonly ConcurrencyProtocol _protocol;
    
    /// <summary>
    /// Konstruktor dengan pemilihan protocol
    /// </summary>
    /// <param name="protocol">Protocol yang akan digunakan</param>
    public ConcurrencyControlManager(ConcurrencyProtocol protocol = ConcurrencyProtocol.TwoPhaseeLocking)
    {
        _protocol = protocol;
        
        // Factory pattern: buat manager sesuai protocol yang dipilih
        _protocolManager = protocol switch
        {
            ConcurrencyProtocol.TwoPhaseeLocking => new TwoPhaseeLockingManager(),
            ConcurrencyProtocol.TimestampOrdering => new TimestampOrderingManager(),
            ConcurrencyProtocol.OptimisticValidation => new OptimisticConcurrencyManager(),
            _ => throw new ArgumentException($"Unknown protocol: {protocol}")
        };
        
        Console.WriteLine($"[CCM] ConcurrencyControlManager initialized with protocol: {protocol}");
    }
    
    /// <summary>
    /// Get current protocol being used
    /// </summary>
    public ConcurrencyProtocol GetProtocol() => _protocol;
    
    /// <summary>
    /// Memulai transaksi baru
    /// </summary>
    public int BeginTransaction()
    {
        return _protocolManager.BeginTransaction();
    }
    
    /// <summary>
    /// Memvalidasi apakah aksi pada objek diizinkan
    /// </summary>
    public Response ValidateObject(Common.Transaction.Action action)
    {
        return _protocolManager.ValidateObject(action);
    }
    
    /// <summary>
    /// Mencatat (log) sebuah objek pada transaksi tertentu
    /// </summary>
    public void LogObject(DatabaseObject obj, int transactionId)
    {
        _protocolManager.LogObject(obj, transactionId);
    }
    
    /// <summary>
    /// Mengakhiri transaksi (commit atau abort)
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        return _protocolManager.EndTransaction(transactionId, commit);
    }
    
    /// <summary>
    /// Abort transaksi
    /// </summary>
    public bool AbortTransaction(int transactionId)
    {
        return _protocolManager.AbortTransaction(transactionId);
    }
    
    /// <summary>
    /// Commit transaksi
    /// </summary>
    public bool CommitTransaction(int transactionId)
    {
        return _protocolManager.CommitTransaction(transactionId);
    }
    
    /// <summary>
    /// Mendapatkan status transaksi
    /// </summary>
    public TransactionStatus GetTransactionStatus(int transactionId)
    {
        return _protocolManager.GetTransactionStatus(transactionId);
    }
    
    /// <summary>
    /// Memeriksa apakah transaksi aktif
    /// </summary>
    public bool IsTransactionActive(int transactionId)
    {
        return _protocolManager.IsTransactionActive(transactionId);
    }
}