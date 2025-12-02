using System.Collections.Concurrent;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Fase transaksi dalam Optimistic Concurrency Control
/// </summary>
public enum OptimisticPhase
{
    Reading,    // Fase membaca dan menulis ke local workspace
    Validating, // Fase validasi konflik
    Writing     // Fase menulis ke database
}

/// <summary>
/// State transaksi untuk Optimistic Concurrency Control
/// </summary>
public class OptimisticTransaction
{
    public int TransactionId { get; set; }
    public long StartTimestamp { get; set; }
    public long? ValidationTimestamp { get; set; }
    public long? FinishTimestamp { get; set; }
    public TransactionStatus Status { get; set; }
    public OptimisticPhase Phase { get; set; }
    public DateTime StartedAt { get; set; }

    // Read set: objek yang dibaca oleh transaksi
    public HashSet<string> ReadSet { get; set; } = new();

    // Write set: objek yang ditulis oleh transaksi
    public HashSet<string> WriteSet { get; set; } = new();

    public OptimisticTransaction(int transactionId, long startTimestamp)
    {
        TransactionId = transactionId;
        StartTimestamp = startTimestamp;
        Status = TransactionStatus.Active;
        Phase = OptimisticPhase.Reading;
        StartedAt = DateTime.UtcNow;
    }
}

/// <summary>
/// Optimistic Concurrency Control (OCC) / Validation-based Protocol Manager
/// Menggunakan tiga fase: Read, Validation, Write
/// </summary>
public class OptimisticConcurrencyManager : IConcurrencyControlManager
{
    private int _nextTransactionId = 1;
    private long _nextTimestamp = 1;
    private readonly object _lockObject = new();

    // Menyimpan state transaksi
    private readonly ConcurrentDictionary<int, OptimisticTransaction> _transactions;

    // Menyimpan transaksi yang sudah committed (untuk backward validation)
    private readonly ConcurrentDictionary<int, OptimisticTransaction> _committedTransactions;

    // Object access log untuk audit trail
    private readonly ConcurrentDictionary<int, List<(DatabaseObject obj, DateTime accessedAt)>> _objectAccessLog;

    public OptimisticConcurrencyManager()
    {
        _transactions = new ConcurrentDictionary<int, OptimisticTransaction>();
        _committedTransactions = new ConcurrentDictionary<int, OptimisticTransaction>();
        _objectAccessLog = new ConcurrentDictionary<int, List<(DatabaseObject, DateTime)>>();
        Console.WriteLine("[OCC] OptimisticConcurrencyManager initialized");
    }

    /// <summary>
    /// Memulai transaksi baru
    /// </summary>
    public int BeginTransaction()
    {
        lock (_lockObject)
        {
            int transactionId = Interlocked.Increment(ref _nextTransactionId);
            long timestamp = Interlocked.Increment(ref _nextTimestamp);

            var txn = new OptimisticTransaction(transactionId, timestamp);
            _transactions.TryAdd(transactionId, txn);

            Console.WriteLine($"[OCC] Transaction T{transactionId} STARTED (Reading Phase) at TS={timestamp}");
            return transactionId;
        }
    }

    /// <summary>
    /// Memvalidasi aksi dalam fase Reading (selalu allowed, track read/write set)
    /// </summary>
    public Response ValidateObject(Common.Transaction.Action action)
    {
        Console.WriteLine($"[OCC] ValidateObject - T{action.TransactionId}: {action.Type} on {action.DatabaseObject.ToQualifiedString()}");

        if (!_transactions.TryGetValue(action.TransactionId, out var txn))
        {
            Console.WriteLine($"[OCC] ERROR: Transaction T{action.TransactionId} not found");
            return Response.CreateDenied(action.TransactionId, "Transaction not found", action.DatabaseObject, action.Type);
        }

        if (txn.Status != TransactionStatus.Active)
        {
            Console.WriteLine($"[OCC] ERROR: Transaction T{action.TransactionId} is not active (Status: {txn.Status})");
            return Response.CreateDenied(action.TransactionId, $"Transaction is {txn.Status}", action.DatabaseObject, action.Type);
        }

        // Dalam fase Reading, semua operasi allowed (optimistic approach)
        // Kita hanya track read set dan write set
        string objectKey = action.DatabaseObject.ToQualifiedString();

        lock (txn)
        {
            if (action.Type == Common.Transaction.Action.ActionType.Read)
            {
                txn.ReadSet.Add(objectKey);
                Console.WriteLine($"[OCC] GRANTED: T{txn.TransactionId} READ {objectKey} - Added to ReadSet (size={txn.ReadSet.Count})");
            }
            else // Write, Insert, Update, Delete
            {
                txn.WriteSet.Add(objectKey);
                Console.WriteLine($"[OCC] GRANTED: T{txn.TransactionId} WRITE {objectKey} - Added to WriteSet (size={txn.WriteSet.Count})");
            }
        }

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
        Console.WriteLine($"[OCC] Logged object access: T{transactionId} accessed {obj.ToQualifiedString()}");
    }

    /// <summary>
    /// Mengakhiri transaksi dengan validasi (untuk commit) atau langsung abort
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[OCC] EndTransaction - T{transactionId} ({(commit ? "COMMIT" : "ABORT")})");

        if (!_transactions.TryGetValue(transactionId, out var txn))
        {
            Console.WriteLine($"[OCC] ERROR: Transaction T{transactionId} not found");
            return false;
        }

        if (commit)
        {
            // Fase Validation
            txn.Phase = OptimisticPhase.Validating;
            txn.ValidationTimestamp = Interlocked.Increment(ref _nextTimestamp);
            Console.WriteLine($"[OCC] Transaction T{transactionId} -> Validation Phase at TS={txn.ValidationTimestamp}");

            // Lakukan validasi terhadap transaksi yang sudah committed
            if (!ValidateTransaction(txn))
            {
                Console.WriteLine($"[OCC] VALIDATION FAILED: T{transactionId} - Aborting due to conflicts");
                txn.Status = TransactionStatus.Failed;
                txn.Status = TransactionStatus.Aborted;
                txn.Status = TransactionStatus.Terminated;

                _objectAccessLog.TryRemove(transactionId, out _);
                return false;
            }

            Console.WriteLine($"[OCC] VALIDATION PASSED: T{transactionId}");

            // Fase Write
            txn.Phase = OptimisticPhase.Writing;
            txn.Status = TransactionStatus.PartiallyCommitted;
            Console.WriteLine($"[OCC] Transaction T{transactionId} -> Writing Phase");

            // Commit
            txn.FinishTimestamp = Interlocked.Increment(ref _nextTimestamp);
            txn.Status = TransactionStatus.Committed;
            Console.WriteLine($"[OCC] Transaction T{transactionId} -> Committed at TS={txn.FinishTimestamp}");

            // Simpan ke committed transactions untuk validasi transaksi lain
            _committedTransactions.TryAdd(transactionId, txn);

            // Clean up old committed transactions (keep last 100)
            if (_committedTransactions.Count > 100)
            {
                var oldestTxn = _committedTransactions.OrderBy(kvp => kvp.Value.FinishTimestamp).FirstOrDefault();
                _committedTransactions.TryRemove(oldestTxn.Key, out _);
            }
        }
        else
        {
            // Abort langsung
            txn.Status = TransactionStatus.Failed;
            Console.WriteLine($"[OCC] Transaction T{transactionId} -> Failed");

            txn.Status = TransactionStatus.Aborted;
            Console.WriteLine($"[OCC] Transaction T{transactionId} -> Aborted");
        }

        txn.Status = TransactionStatus.Terminated;
        Console.WriteLine($"[OCC] Transaction T{transactionId} -> Terminated");

        // Clean up access log
        _objectAccessLog.TryRemove(transactionId, out _);

        return true;
    }

    /// <summary>
    /// Validasi transaksi menggunakan Backward Validation
    /// Cek konflik dengan transaksi yang sudah committed sejak T mulai
    /// </summary>
    private bool ValidateTransaction(OptimisticTransaction txn)
    {
        Console.WriteLine($"[OCC] Validating T{txn.TransactionId} against {_committedTransactions.Count} committed transactions");

        // Backward Validation: cek terhadap semua transaksi yang committed
        // sejak transaksi ini mulai
        foreach (var committedTxn in _committedTransactions.Values)
        {
            // Skip jika committed transaction selesai sebelum T mulai
            if (committedTxn.FinishTimestamp < txn.StartTimestamp)
                continue;

            // Cek Read-Write conflict: WriteSet(Ti) ∩ ReadSet(T) ≠ ∅
            var readWriteConflict = committedTxn.WriteSet.Intersect(txn.ReadSet).ToList();
            if (readWriteConflict.Any())
            {
                Console.WriteLine($"[OCC] CONFLICT: T{txn.TransactionId} has Read-Write conflict with T{committedTxn.TransactionId}");
                Console.WriteLine($"[OCC]           Conflicting objects: {string.Join(", ", readWriteConflict)}");
                return false;
            }

            // Cek Write-Write conflict: WriteSet(Ti) ∩ WriteSet(T) ≠ ∅
            var writeWriteConflict = committedTxn.WriteSet.Intersect(txn.WriteSet).ToList();
            if (writeWriteConflict.Any())
            {
                Console.WriteLine($"[OCC] CONFLICT: T{txn.TransactionId} has Write-Write conflict with T{committedTxn.TransactionId}");
                Console.WriteLine($"[OCC]           Conflicting objects: {string.Join(", ", writeWriteConflict)}");
                return false;
            }
        }

        Console.WriteLine($"[OCC] No conflicts found for T{txn.TransactionId}");
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
    /// Commit transaksi (dengan validasi)
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
