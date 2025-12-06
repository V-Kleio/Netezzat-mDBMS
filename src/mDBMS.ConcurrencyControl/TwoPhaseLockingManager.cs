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
/// Two-Phase Locking Manager menggunakan 2PL Protocol dengan Deadlock Detection
/// </summary>
public class TwoPhaseLockingManager : IConcurrencyControlManager
{
    private int _nextTransactionId = 1;
    private readonly object _lockObject = new();

    // Menyimpan state transaksi
    private readonly ConcurrentDictionary<int, TransactionState> _transactions;

    // Lock table: key = object identifier, value = list of locks pada object tersebut
    private readonly ConcurrentDictionary<string, List<Lock>> _lockTable;

    // Wait-for graph untuk deadlock detection: key = waiting transaction, value = set of transactions it's waiting for
    private readonly ConcurrentDictionary<int, HashSet<int>> _waitForGraph;

    // Object access log untuk audit trail
    private readonly ConcurrentDictionary<int, List<(DatabaseObject obj, DateTime accessedAt)>> _objectAccessLog;

    public TwoPhaseLockingManager()
    {
        _transactions = new ConcurrentDictionary<int, TransactionState>();
        _lockTable = new ConcurrentDictionary<string, List<Lock>>();
        _waitForGraph = new ConcurrentDictionary<int, HashSet<int>>();
        _objectAccessLog = new ConcurrentDictionary<int, List<(DatabaseObject, DateTime)>>();
        Console.WriteLine("[2PL] TwoPhaseeLockingManager initialized with Two-Phase Locking Protocol + Deadlock Detection");
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

            Console.WriteLine($"[2PL] Transaction T{transactionId} STARTED (Growing Phase)");
            return transactionId;
        }
    }

    /// <summary>
    /// Memvalidasi apakah aksi pada objek diizinkan menggunakan 2PL
    /// </summary>
    public Response ValidateObject(Common.Transaction.Action action)
    {
        Console.WriteLine($"[2PL] ValidateObject - T{action.TransactionId}: {action.Type} on {action.DatabaseObject.ToQualifiedString()}");

        if (!_transactions.TryGetValue(action.TransactionId, out var txnState))
        {
            Console.WriteLine($"[2PL] ERROR: Transaction T{action.TransactionId} not found");
            return Response.CreateDenied(action.TransactionId, "Transaction not found", action.DatabaseObject, action.Type);
        }

        if (txnState.Status != TransactionStatus.Active && txnState.Status != TransactionStatus.Waiting)
        {
            Console.WriteLine($"[2PL] ERROR: Transaction T{action.TransactionId} is not active (Status: {txnState.Status})");
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
    /// Mencatat (log) sebuah objek pada transaksi tertentu
    /// </summary>
    public void LogObject(DatabaseObject obj, int transactionId)
    {
        var log = _objectAccessLog.GetOrAdd(transactionId, _ => new List<(DatabaseObject, DateTime)>());
        lock (log)
        {
            log.Add((obj, DateTime.UtcNow));
        }
        Console.WriteLine($"[2PL] Logged object access: T{transactionId} accessed {obj.ToQualifiedString()}");
    }

    /// <summary>
    /// Mengakhiri transaksi (commit atau abort)
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[2PL] EndTransaction - T{transactionId} ({(commit ? "COMMIT" : "ABORT")})");

        if (!_transactions.TryGetValue(transactionId, out var txnState))
        {
            Console.WriteLine($"[2PL] ERROR: Transaction T{transactionId} not found");
            return false;
        }

        if (commit)
        {
            // State transition: Active -> PartiallyCommitted -> Committed -> Terminated
            txnState.Status = TransactionStatus.PartiallyCommitted;
            Console.WriteLine($"[2PL] Transaction T{transactionId} -> PartiallyCommitted");

            // Simulate commit processing (flush to disk, etc.)
            txnState.Status = TransactionStatus.Committed;
            Console.WriteLine($"[2PL] Transaction T{transactionId} -> Committed");
        }
        else
        {
            // State transition: Active -> Failed -> Aborted -> Terminated
            txnState.Status = TransactionStatus.Failed;
            Console.WriteLine($"[2PL] Transaction T{transactionId} -> Failed");

            // Perform rollback (would restore data from before-images)
            txnState.Status = TransactionStatus.Aborted;
            Console.WriteLine($"[2PL] Transaction T{transactionId} -> Aborted");
        }

        // Masuk ke shrinking phase dan release semua locks
        txnState.InGrowingPhase = false;
        ReleaseAllLocks(transactionId);

        // Final state: Terminated
        txnState.Status = TransactionStatus.Terminated;
        Console.WriteLine($"[2PL] Transaction T{transactionId} -> Terminated - All locks released");

        // Clean up wait-for graph and access log
        _waitForGraph.TryRemove(transactionId, out _);
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
            Console.WriteLine($"[2PL] DENIED: T{transactionId} in SHRINKING PHASE - cannot acquire new locks");
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
                        Console.WriteLine($"[2PL] WAIT: T{transactionId} cannot upgrade S→X lock - other txns hold S-locks");
                        return Response.CreateWaiting(transactionId, "Cannot upgrade lock - conflict with other shared locks", obj, actionType);
                    }

                    // Upgrade
                    existingLock.Type = LockType.Exclusive;
                    Console.WriteLine($"[2PL] GRANTED: T{transactionId} UPGRADED S→X lock on {objectKey}");
                    return Response.CreateAllowed(transactionId, obj, actionType);
                }

                Console.WriteLine($"[2PL] GRANTED: T{transactionId} already holds {existingLock.Type} lock on {objectKey}");
                return Response.CreateAllowed(transactionId, obj, actionType);
            }

            // Cek kompatibilitas dengan locks yang sudah ada
            if (IsLockCompatible(locks, lockType, transactionId))
            {
                // Grant
                var newLock = new Lock(transactionId, lockType, obj);
                locks.Add(newLock);
                txnState.HeldLocks.Add(objectKey);

                // Reset to Active if was Waiting
                if (txnState.Status == TransactionStatus.Waiting)
                {
                    txnState.Status = TransactionStatus.Active;
                }

                Console.WriteLine($"[2PL] GRANTED: T{transactionId} acquired {lockType} lock on {objectKey}");
                return Response.CreateAllowed(transactionId, obj, actionType);
            }
            else
            {
                // Lock conflict --> check for deadlock
                var conflictingTxns = locks.Select(l => l.TransactionId).Where(tid => tid != transactionId).Distinct().ToList();

                // Update wait-for graph
                var waitingFor = _waitForGraph.GetOrAdd(transactionId, _ => new HashSet<int>());
                foreach (var conflictTxn in conflictingTxns)
                {
                    waitingFor.Add(conflictTxn);
                }

                // Detect deadlock
                if (DetectDeadlock(transactionId, out int victimTxnId))
                {
                    Console.WriteLine($"[2PL] DEADLOCK DETECTED: T{transactionId} aborted");
                    AbortTransaction(victimTxnId);
                    return Response.CreateDenied(transactionId, "Deadlock detected - transaction aborted", obj, actionType);
                }

                // Set transaction to waiting state
                txnState.Status = TransactionStatus.Waiting;

                Console.WriteLine($"[2PL] WAIT: T{transactionId} waiting for {lockType} lock on {objectKey}");
                Console.WriteLine($"[2PL]       Held by: T{string.Join(", T", conflictingTxns)}");

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

            Console.WriteLine($"[2PL] Released {lockCount} lock(s) for T{transactionId}");

            // Remove from wait-for graph (others no longer wait for this transaction)
            foreach (var kvp in _waitForGraph)
            {
                kvp.Value.Remove(transactionId);
            }
        }
    }

    /// <summary>
    /// Deteksi deadlock menggunakan cycle detection pada wait-for graph
    /// </summary>
    private bool DetectDeadlock(int transactionId, out int victimTxnId)
    {
        var visited = new HashSet<int>();
        var recursionStack = new HashSet<int>();
        var cycleParticipants = new HashSet<int>();

        if (HasCycle(transactionId, visited, recursionStack, cycleParticipants))
        {
            // Select youngest transaction (highest ID) as victim
            victimTxnId = cycleParticipants.Max();
            return true;
        }

        victimTxnId = -1;
        return false;
    }

    /// <summary>
    /// DFS untuk mendeteksi cycle dalam wait-for graph
    /// </summary>
    private bool HasCycle(int txnId, HashSet<int> visited, HashSet<int> recursionStack, HashSet<int> cycleParticipants)
    {
        if (recursionStack.Contains(txnId))
        {
            cycleParticipants.Add(txnId);
            return true;
        }

        if (visited.Contains(txnId))
            return false;

        visited.Add(txnId);
        recursionStack.Add(txnId);

        if (_waitForGraph.TryGetValue(txnId, out var waitingFor))
        {
            foreach (var waitTxn in waitingFor.ToList())
            {
                if (HasCycle(waitTxn, visited, recursionStack, cycleParticipants))
                {
                    cycleParticipants.Add(txnId);
                    recursionStack.Remove(txnId);
                    return true;
                }
            }
        }

        recursionStack.Remove(txnId);
        return false;
    }
}
