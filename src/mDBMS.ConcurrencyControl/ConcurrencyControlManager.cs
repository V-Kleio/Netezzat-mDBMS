using System.Collections.Concurrent;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Enum untuk tipe lock dalam 2PL
/// </summary>
public enum LockType
{
    Shared,
    Exclusive
}

/// <summary>
/// Object lock yang dipegang oleh transaksi
/// </summary>
public class Lock
{
    public int TransactionId { get; set; }
    public LockType Type { get; set; }
    public DatabaseObject DatabaseObject { get; set; }
    public DateTime AcquiredAt { get; set; }

    public Lock(int transactionId, LockType type, DatabaseObject obj)
    {
        TransactionId = transactionId;
        Type = type;
        DatabaseObject = obj;
        AcquiredAt = DateTime.UtcNow;
    }
}

/// <summary>
/// State transaksi dalam 2PL Protocol
/// </summary>
public class TransactionState
{
    public int TransactionId { get; set; }
    public TransactionStatus Status { get; set; }
    public bool InGrowingPhase { get; set; } = true; // true = growing phase, false = shrinking phase
    public HashSet<string> HeldLocks { get; set; } = new(); // daftar object keys yang di-lock
    public DateTime StartedAt { get; set; }

    public TransactionState(int transactionId)
    {
        TransactionId = transactionId;
        Status = TransactionStatus.Active;
        StartedAt = DateTime.UtcNow;
    }
}

/// <summary>
/// Concurrency Control Manager menggunakan 2PL Protocol
/// </summary>
public class ConcurrencyControlManager : IConcurrencyControlManager
{
    private int _nextTransactionId = 1;
    private readonly object _lockObject = new();

    // Menyimpan state transaksi
    private readonly ConcurrentDictionary<int, TransactionState> _transactions;

    // Lock table: key = object identifier, value = list of locks pada object tersebut
    private readonly ConcurrentDictionary<string, List<Lock>> _lockTable;

    public ConcurrencyControlManager()
    {
        _transactions = new ConcurrentDictionary<int, TransactionState>();
        _lockTable = new ConcurrentDictionary<string, List<Lock>>();
        Console.WriteLine("[CCM] ConcurrencyControlManager initialized with Two-Phase Locking Protocol");
    }

    /// <summary>
    /// Memulai transaksi baru
    /// </summary>
    public int BeginTransaction()
    {
        lock (_lockObject)
        {
            int transactionId = Interlocked.Increment(ref _nextTransactionId);
            var txnState = new TransactionState(transactionId);
            _transactions.TryAdd(transactionId, txnState);

            Console.WriteLine($"[CCM] Transaction T{transactionId} STARTED (Growing Phase)");
            return transactionId;
        }
    }

    /// <summary>
    /// Memvalidasi apakah aksi pada objek diizinkan menggunakan 2PL
    /// </summary>
    public Response ValidateObject(Common.Transaction.Action action)
    {
        Console.WriteLine($"[CCM] ValidateObject - T{action.TransactionId}: {action.Type} on {action.DatabaseObject.ToQualifiedString()}");

        if (!_transactions.TryGetValue(action.TransactionId, out var txnState))
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{action.TransactionId} not found");
            return Response.CreateDenied(action.TransactionId, "Transaction not found", action.DatabaseObject, action.Type);
        }

        if (txnState.Status != TransactionStatus.Active)
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{action.TransactionId} is not active (Status: {txnState.Status})");
            return Response.CreateDenied(action.TransactionId, $"Transaction is {txnState.Status}", action.DatabaseObject, action.Type);
        }

        // Tipe lock yang dibutuhkan berdasarkan action type
        LockType requiredLockType = action.Type == Common.Transaction.Action.ActionType.Read 
            ? LockType.Shared 
            : LockType.Exclusive;

        // Acquire lock
        return AcquireLock(action.TransactionId, action.DatabaseObject, requiredLockType, action.Type);
    }

    /// <summary>
    /// Mengakhiri transaksi (commit atau abort)
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[CCM] EndTransaction - T{transactionId} ({(commit ? "COMMIT" : "ABORT")})");

        if (!_transactions.TryGetValue(transactionId, out var txnState))
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{transactionId} not found");
            return false;
        }

        // Masuk ke shrinking phase dan release semua locks
        txnState.InGrowingPhase = false;
        ReleaseAllLocks(transactionId);

        // Update status transaksi
        var newStatus = commit ? TransactionStatus.Committed : TransactionStatus.Aborted;
        txnState.Status = newStatus;

        Console.WriteLine($"[CCM] Transaction T{transactionId} {(commit ? "COMMITTED" : "ABORTED")} - All locks released");
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
        if (_transactions.TryGetValue(transactionId, out var txnState))
        {
            return txnState.Status;
        }
        return TransactionStatus.Aborted;
    }

    /// <summary>
    /// Memeriksa apakah transaksi aktif
    /// </summary>
    public bool IsTransactionActive(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var txnState))
        {
            return txnState.Status == TransactionStatus.Active;
        }
        return false;
    }

    /// <summary>
    /// Mencoba untuk acquire lock pada database object
    /// </summary>
    private Response AcquireLock(int transactionId, DatabaseObject obj, LockType lockType, Common.Transaction.Action.ActionType actionType)
    {
        if (!_transactions.TryGetValue(transactionId, out var txnState))
        {
            return Response.CreateDenied(transactionId, "Transaction not found", obj, actionType);
        }

        // Cek apakah shrinking phase
        if (!txnState.InGrowingPhase)
        {
            Console.WriteLine($"[CCM] DENIED: T{transactionId} in SHRINKING PHASE - cannot acquire new locks");
            return Response.CreateDenied(transactionId, "Transaction in shrinking phase - 2PL violation", obj, actionType);
        }

        string objectKey = obj.ToQualifiedString();

        lock (_lockObject)
        {
            // Dapatkan atau buat lock list untuk object
            var locks = _lockTable.GetOrAdd(objectKey, _ => new List<Lock>());
            
            // Cek apakah transaksi sudah punya lock pada object
            var existingLock = locks.FirstOrDefault(l => l.TransactionId == transactionId);
            
            if (existingLock != null)
            {
                // Lock upgrade
                if (existingLock.Type == LockType.Shared && lockType == LockType.Exclusive)
                {
                    // Cek apakah ada transaksi lain yang pegang S-lock
                    if (locks.Count > 1)
                    {
                        Console.WriteLine($"[CCM] WAIT: T{transactionId} cannot upgrade S→X lock - other txns hold S-locks");
                        return Response.CreateWaiting(transactionId, "Cannot upgrade lock - conflict with other shared locks", obj, actionType);
                    }

                    // Upgrade
                    existingLock.Type = LockType.Exclusive;
                    Console.WriteLine($"[CCM] GRANTED: T{transactionId} UPGRADED S→X lock on {objectKey}");
                    return Response.CreateAllowed(transactionId, obj, actionType);
                }

                Console.WriteLine($"[CCM] GRANTED: T{transactionId} already holds {existingLock.Type} lock on {objectKey}");
                return Response.CreateAllowed(transactionId, obj, actionType);
            }

            // Cek kompatibilitas dengan locks yang sudah ada
            if (IsLockCompatible(locks, lockType, transactionId))
            {
                // Grant
                var newLock = new Lock(transactionId, lockType, obj);
                locks.Add(newLock);
                txnState.HeldLocks.Add(objectKey);

                Console.WriteLine($"[CCM] GRANTED: T{transactionId} acquired {lockType} lock on {objectKey}");
                return Response.CreateAllowed(transactionId, obj, actionType);
            }
            else
            {
                // Lock conflict --> transaction wait
                var conflictingTxns = locks.Select(l => l.TransactionId).Where(tid => tid != transactionId).ToHashSet();
                
                Console.WriteLine($"[CCM] WAIT: T{transactionId} waiting for {lockType} lock on {objectKey}");
                Console.WriteLine($"[CCM]       Held by: T{string.Join(", T", conflictingTxns)}");
                
                return Response.CreateWaiting(transactionId, $"Lock conflict - waiting for other transactions", obj, actionType);
            }
        }
    }

    /// <summary>
    /// Memeriksa kompatibilitas lock berdasarkan compatibility matrix
    /// </summary>
    private bool IsLockCompatible(List<Lock> existingLocks, LockType requestedType, int transactionId)
    {
        if (existingLocks.Count == 0)
            return true;

        // Filter locks dari transaksi lain (ignore locks dari transaksi sendiri)
        var otherLocks = existingLocks.Where(l => l.TransactionId != transactionId).ToList();
        
        if (otherLocks.Count == 0)
            return true;

        if (requestedType == LockType.Shared)
        {
            // S-lock compatible w/ other s-locks
            return otherLocks.All(l => l.Type == LockType.Shared);
        }
        else // Exclusive
        {
            return false;
        }
    }

    /// <summary>
    /// Release semua locks yang dipegang oleh transaksi (shrinking phase)
    /// </summary>
    private void ReleaseAllLocks(int transactionId)
    {
        lock (_lockObject)
        {
            if (!_transactions.TryGetValue(transactionId, out var txnState))
                return;

            int lockCount = txnState.HeldLocks.Count;

            foreach (var objectKey in txnState.HeldLocks.ToList())
            {
                if (_lockTable.TryGetValue(objectKey, out var locks))
                {
                    locks.RemoveAll(l => l.TransactionId == transactionId);
                    
                    // Hapus entry jika tidak ada lock lagi
                    if (locks.Count == 0)
                    {
                        _lockTable.TryRemove(objectKey, out _);
                    }
                }
            }

            txnState.HeldLocks.Clear();
            
            Console.WriteLine($"[CCM] Released {lockCount} lock(s) for T{transactionId}");
        }
    }
}
