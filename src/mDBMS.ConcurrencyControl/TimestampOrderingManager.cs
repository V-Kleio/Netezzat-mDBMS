using System.Collections.Concurrent;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Object dengan timestamp untuk Timestamp Ordering Protocol
/// </summary>
public class TimestampedObject
{
    public string ObjectKey { get; set; }
    public long ReadTimestamp { get; set; }  // RTS(X) - timestamp transaksi terakhir yang read
    public long WriteTimestamp { get; set; } // WTS(X) - timestamp transaksi terakhir yang write
    
    public TimestampedObject(string objectKey)
    {
        ObjectKey = objectKey;
        ReadTimestamp = 0;
        WriteTimestamp = 0;
    }
}

/// <summary>
/// State transaksi untuk Timestamp Ordering Protocol
/// </summary>
public class TimestampTransaction
{
    public int TransactionId { get; set; }
    public long Timestamp { get; set; }
    public TransactionStatus Status { get; set; }
    public DateTime StartedAt { get; set; }
    
    public TimestampTransaction(int transactionId, long timestamp)
    {
        TransactionId = transactionId;
        Timestamp = timestamp;
        Status = TransactionStatus.Active;
        StartedAt = DateTime.UtcNow;
    }
}

/// <summary>
/// Timestamp Ordering Protocol Manager
/// Menggunakan timestamp untuk menentukan serialization order transaksi
/// </summary>
public class TimestampOrderingManager : IConcurrencyControlManager
{
    private int _nextTransactionId = 1;
    private long _nextTimestamp = 1;
    private readonly object _lockObject = new();
    
    // Menyimpan state transaksi dengan timestamp
    private readonly ConcurrentDictionary<int, TimestampTransaction> _transactions;
    
    // Menyimpan timestamp untuk setiap database object
    private readonly ConcurrentDictionary<string, TimestampedObject> _timestampTable;
    
    // Object access log untuk audit trail
    private readonly ConcurrentDictionary<int, List<(DatabaseObject obj, DateTime accessedAt)>> _objectAccessLog;
    
    public TimestampOrderingManager()
    {
        _transactions = new ConcurrentDictionary<int, TimestampTransaction>();
        _timestampTable = new ConcurrentDictionary<string, TimestampedObject>();
        _objectAccessLog = new ConcurrentDictionary<int, List<(DatabaseObject, DateTime)>>();
        Console.WriteLine("[TO] TimestampOrderingManager initialized");
    }
    
    /// <summary>
    /// Memulai transaksi baru dengan timestamp unik
    /// </summary>
    public int BeginTransaction()
    {
        lock (_lockObject)
        {
            int transactionId = Interlocked.Increment(ref _nextTransactionId);
            long timestamp = Interlocked.Increment(ref _nextTimestamp);
            
            var txn = new TimestampTransaction(transactionId, timestamp);
            _transactions.TryAdd(transactionId, txn);
            
            Console.WriteLine($"[TO] Transaction T{transactionId} STARTED with TS={timestamp}");
            return transactionId;
        }
    }
    
    /// <summary>
    /// Memvalidasi aksi menggunakan Timestamp Ordering rules
    /// </summary>
    public Response ValidateObject(Common.Transaction.Action action)
    {
        Console.WriteLine($"[TO] ValidateObject - T{action.TransactionId}: {action.Type} on {action.DatabaseObject.ToQualifiedString()}");
        
        if (!_transactions.TryGetValue(action.TransactionId, out var txn))
        {
            Console.WriteLine($"[TO] ERROR: Transaction T{action.TransactionId} not found");
            return Response.CreateDenied(action.TransactionId, "Transaction not found", action.DatabaseObject, action.Type);
        }
        
        if (txn.Status != TransactionStatus.Active)
        {
            Console.WriteLine($"[TO] ERROR: Transaction T{action.TransactionId} is not active (Status: {txn.Status})");
            return Response.CreateDenied(action.TransactionId, $"Transaction is {txn.Status}", action.DatabaseObject, action.Type);
        }
        
        string objectKey = action.DatabaseObject.ToQualifiedString();
        var tsObj = _timestampTable.GetOrAdd(objectKey, key => new TimestampedObject(key));
        
        lock (tsObj)
        {
            if (action.Type == Common.Transaction.Action.ActionType.Read)
            {
                return ValidateRead(txn, tsObj, action);
            }
            else // Write, Insert, Update, Delete
            {
                return ValidateWrite(txn, tsObj, action);
            }
        }
    }
    
    /// <summary>
    /// Validasi operasi READ menggunakan Timestamp Ordering
    /// Rule: Jika TS(T) < WTS(X), abort T (membaca data yang sudah obsolete)
    /// </summary>
    private Response ValidateRead(TimestampTransaction txn, TimestampedObject tsObj, Common.Transaction.Action action)
    {
        // Cek apakah transaksi mencoba membaca data yang sudah di-overwrite oleh transaksi lebih baru
        if (txn.Timestamp < tsObj.WriteTimestamp)
        {
            Console.WriteLine($"[TO] ABORT: T{txn.TransactionId} (TS={txn.Timestamp}) reading {tsObj.ObjectKey} with WTS={tsObj.WriteTimestamp}");
            Console.WriteLine($"[TO]        Reason: TS(T) < WTS(X) - reading obsolete data");
            
            AbortTransaction(txn.TransactionId);
            return Response.CreateDenied(txn.TransactionId, 
                $"Timestamp ordering violation: TS({txn.Timestamp}) < WTS({tsObj.WriteTimestamp})", 
                action.DatabaseObject, action.Type);
        }
        
        // Update RTS(X) = max(RTS(X), TS(T))
        if (txn.Timestamp > tsObj.ReadTimestamp)
        {
            tsObj.ReadTimestamp = txn.Timestamp;
            Console.WriteLine($"[TO] GRANTED: T{txn.TransactionId} READ {tsObj.ObjectKey} - Updated RTS={tsObj.ReadTimestamp}");
        }
        else
        {
            Console.WriteLine($"[TO] GRANTED: T{txn.TransactionId} READ {tsObj.ObjectKey} - RTS unchanged={tsObj.ReadTimestamp}");
        }
        
        return Response.CreateAllowed(txn.TransactionId, action.DatabaseObject, action.Type);
    }
    
    /// <summary>
    /// Validasi operasi WRITE menggunakan Timestamp Ordering dengan Thomas Write Rule
    /// Rule 1: Jika TS(T) < RTS(X), abort T (terlalu terlambat untuk write)
    /// Rule 2: Jika TS(T) < WTS(X), skip write (Thomas Write Rule - write sudah obsolete)
    /// </summary>
    private Response ValidateWrite(TimestampTransaction txn, TimestampedObject tsObj, Common.Transaction.Action action)
    {
        // Rule 1: Cek apakah transaksi mencoba menulis data yang sudah dibaca oleh transaksi lebih baru
        if (txn.Timestamp < tsObj.ReadTimestamp)
        {
            Console.WriteLine($"[TO] ABORT: T{txn.TransactionId} (TS={txn.Timestamp}) writing {tsObj.ObjectKey} with RTS={tsObj.ReadTimestamp}");
            Console.WriteLine($"[TO]        Reason: TS(T) < RTS(X) - too late to write");
            
            AbortTransaction(txn.TransactionId);
            return Response.CreateDenied(txn.TransactionId, 
                $"Timestamp ordering violation: TS({txn.Timestamp}) < RTS({tsObj.ReadTimestamp})", 
                action.DatabaseObject, action.Type);
        }
        
        // Rule 2: Thomas Write Rule - jika write sudah obsolete, skip saja (tidak perlu abort)
        if (txn.Timestamp < tsObj.WriteTimestamp)
        {
            Console.WriteLine($"[TO] SKIP WRITE: T{txn.TransactionId} (TS={txn.Timestamp}) writing {tsObj.ObjectKey} with WTS={tsObj.WriteTimestamp}");
            Console.WriteLine($"[TO]             Reason: Thomas Write Rule - write is obsolete, but transaction continues");
            
            // Tetap allowed, tapi write di-skip (implementasi actual write ada di storage layer)
            return Response.CreateAllowed(txn.TransactionId, action.DatabaseObject, action.Type);
        }
        
        // Update WTS(X) = TS(T)
        tsObj.WriteTimestamp = txn.Timestamp;
        Console.WriteLine($"[TO] GRANTED: T{txn.TransactionId} WRITE {tsObj.ObjectKey} - Updated WTS={tsObj.WriteTimestamp}");
        
        return Response.CreateAllowed(txn.TransactionId, action.DatabaseObject, action.Type);
    }
    
    /// <summary>
    /// Mencatat (log) sebuah objek pada transaksi tertentu
    /// </summary>
    public void LogObject(DatabaseObject obj, int transactionId)
    {
        var log = _objectAccessLog.GetOrAdd(transactionId, _ => new List<(DatabaseObject, DateTime)>());
        lock (log)
        {
            log.Add((obj, DateTime.UtcNow));
        }
        Console.WriteLine($"[TO] Logged object access: T{transactionId} accessed {obj.ToQualifiedString()}");
    }
    
    /// <summary>
    /// Mengakhiri transaksi (commit atau abort)
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[TO] EndTransaction - T{transactionId} ({(commit ? "COMMIT" : "ABORT")})");
        
        if (!_transactions.TryGetValue(transactionId, out var txn))
        {
            Console.WriteLine($"[TO] ERROR: Transaction T{transactionId} not found");
            return false;
        }
        
        if (commit)
        {
            txn.Status = TransactionStatus.PartiallyCommitted;
            Console.WriteLine($"[TO] Transaction T{transactionId} -> PartiallyCommitted");
            
            txn.Status = TransactionStatus.Committed;
            Console.WriteLine($"[TO] Transaction T{transactionId} -> Committed");
        }
        else
        {
            txn.Status = TransactionStatus.Failed;
            Console.WriteLine($"[TO] Transaction T{transactionId} -> Failed");
            
            txn.Status = TransactionStatus.Aborted;
            Console.WriteLine($"[TO] Transaction T{transactionId} -> Aborted");
        }
        
        txn.Status = TransactionStatus.Terminated;
        Console.WriteLine($"[TO] Transaction T{transactionId} -> Terminated");
        
        // Clean up access log
        _objectAccessLog.TryRemove(transactionId, out _);
        
        return true;
    }
    
    /// <summary>
    /// Abort transaksi
    /// </summary>
    public bool AbortTransaction(int transactionId)
    {
        return EndTransaction(transactionId, commit: false);
    }
    
    /// <summary>
    /// Commit transaksi
    /// </summary>
    public bool CommitTransaction(int transactionId)
    {
        return EndTransaction(transactionId, commit: true);
    }
    
    /// <summary>
    /// Mendapatkan status transaksi
    /// </summary>
    public TransactionStatus GetTransactionStatus(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var txn))
        {
            return txn.Status;
        }
        return TransactionStatus.Aborted;
    }
    
    /// <summary>
    /// Memeriksa apakah transaksi aktif
    /// </summary>
    public bool IsTransactionActive(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var txn))
        {
            return txn.Status == TransactionStatus.Active;
        }
        return false;
    }
}
