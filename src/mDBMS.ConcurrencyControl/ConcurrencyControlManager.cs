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
        this.TransactionId = transactionId;
        this.Type = type;
        this.DatabaseObject = obj;
        this.AcquiredAt = DateTime.UtcNow;
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

    public HashSet<int> WaitingFor { get; set; } = new(); // menyimpan ID transaksi yang sedang ditunggu oleh transaksi ini

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
    private readonly IConcurrencyControlManager _protocolManager;
    private readonly ConcurrencyProtocol _protocol;private readonly ConcurrentDictionary<int, TransactionState> _transactions;
    private readonly object _lockObject = new();
    private readonly ConcurrentDictionary<string, List<Lock>> _lockTable;
    private readonly ConcurrentDictionary<int, List<(DatabaseObject obj, DateTime accessedAt)>> _objectAccessLog;

    /// <summary>
    /// Konstruktor dengan pemilihan protocol
    /// </summary>
    /// <param name="protocol">Protocol yang akan digunakan</param>
    public ConcurrencyControlManager(ConcurrencyProtocol protocol = ConcurrencyProtocol.TwoPhaseeLocking)
    {
        _transactions = new ConcurrentDictionary<int, TransactionState>();
        _lockTable = new ConcurrentDictionary<string, List<Lock>>();
        _objectAccessLog = new ConcurrentDictionary<int, List<(DatabaseObject, DateTime)>>();
        // Console.WriteLine("[CCM] ConcurrencyControlManager initialized with Two-Phase Locking Protocol");

        // _protocol = protocol;

        // Factory pattern: buat manager sesuai protocol yang dipilih
        _protocolManager = protocol switch
        {
            // ConcurrencyProtocol.TwoPhaseeLocking => new TwoPhaseeLockingManager(),
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
        Console.WriteLine(
            $"[CCM] ValidateObject - T{action.TransactionId}: {action.Type} on {action.DatabaseObject.ToQualifiedString()}");

        // Cek apakah transaksi ada
        if (!_transactions.TryGetValue(action.TransactionId, out var txnState))
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{action.TransactionId} not found");
            return Response.CreateDenied(action.TransactionId, "Transaction not found", action.DatabaseObject, action.Type);
        }

        // Transaksi yang sudah selesai tidak boleh minta lock lagi, harus active atau waiting
        if (txnState.Status != TransactionStatus.Active && txnState.Status != TransactionStatus.Waiting)
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{action.TransactionId} is {txnState.Status}");
            return Response.CreateDenied(action.TransactionId, $"Transaction is {txnState.Status}", action.DatabaseObject, action.Type);
        }

        // Tipe lock yang dibutuhkan berdasarkan action type
        var requiredLockType =
        action.Type == Common.Transaction.Action.ActionType.Read
            ? LockType.Shared
            : LockType.Exclusive;

        // Meminta lock
        var response = AcquireLock(action.TransactionId,action.DatabaseObject,requiredLockType,action.Type);

        // Status transaksi diperbarui
        lock (_lockObject)
        {
            if (response.Status == Response.ResponseStatus.Waiting)
            {
                // Transaksi harus menunggu karena ada konflik lock
                txnState.Status = TransactionStatus.Waiting;
                Console.WriteLine($"[CCM] Transaction T{action.TransactionId} now waiting for lock");
            }
            else if (response.Status == Response.ResponseStatus.Granted)
            {
                // Lock diberikan dan status di update
                if (txnState.Status == TransactionStatus.Waiting)
                {
                    txnState.Status = TransactionStatus.Active;
                    Console.WriteLine($"[CCM] Transaction T{action.TransactionId} back to execution");
                }
            }
        }

        return response;
    }


    /// <summary>
    /// Mengakhiri transaksi (commit atau abort)
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[CCM] EndTransaction - T{transactionId} ({(commit ? "COMMIT" : "ABORT")})");
        // Cek apakah transaksi ada
        if (!_transactions.TryGetValue(transactionId, out var txnState))
        {
            Console.WriteLine($"[CCM] ERROR: Transaction T{transactionId} not found");
            return false;
        }

        lock (_lockObject)
        {
            if (txnState.Status == TransactionStatus.Committed || // Mencegah transaksi yang sudah selesai diakhiri lagi
            txnState.Status == TransactionStatus.Aborted)
            {
                Console.WriteLine($"[CCM] WARNING: Transaction T{transactionId} already finished with status {txnState.Status}");
                return false;
            }

            txnState.Status = commit
            ? TransactionStatus.Committed
            : TransactionStatus.Aborted;

            Console.WriteLine($"[CCM] Transaction T{transactionId} status: {txnState.Status}");

            // Masuk ke shrinking phase, tidak boleh acquire lock lagi
            txnState.InGrowingPhase = false;
        }

        // Release semua locks yang dipegang transaksi
        ReleaseAllLocks(transactionId);

        // Set status akhir transaksi
        lock (_lockObject)
        {
            txnState.Status = commit
                ? TransactionStatus.Committed
                : TransactionStatus.Aborted;
        }

        Console.WriteLine($"[CCM] Transaction T{transactionId} {(commit ? "COMMITTED" : "ABORTED")} - All locks released");
        return true;
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
                // LOGIKA DEADLOCK DETECTION

                // Cari transaksi yang memegang lock yang menyebabkan konflik
                // (Abaikan lock milik diri sendiri kalau ada multiple locks)
                var conflictingTxns = locks
                    .Where(l => l.TransactionId != transactionId)
                    .Select(l => l.TransactionId)
                    .Distinct()
                    .ToList();

                // 2. Cek Deadlock untuk setiap transaksi yang menyebabkan konflik
                foreach (var holderId in conflictingTxns)
                {
                    // Gunakan fungsi deadlock
                    if (DetectDeadlock(transactionId, holderId))
                    {
                        // Terdeteksi Deadlock
                        Console.WriteLine($"[CCM] DEADLOCK DETECTED: T{transactionId} waiting for T{holderId} creates a cycle.");

                        // ABORT Transaksi tsb
                        AbortTransaction(transactionId);

                        return new Response
                        {
                            Allowed = false,
                            TransactionId = transactionId,
                            DatabaseObject = obj,
                            ActionType = actionType,
                            Status = Response.ResponseStatus.Deadlock,
                            Reason = $"Deadlock detected with T{holderId}"
                        };
                    }
                }

                // 3. Jika tidak deadlock, catat bahwa kita menunggu mereka (Update Wait-For Graph)
                foreach (var holderId in conflictingTxns)
                {
                    txnState.WaitingFor.Add(holderId);
                }

                Console.WriteLine($"[CCM] WAIT: T{transactionId} waiting for {lockType} lock on {objectKey}. Holders: {string.Join(",", conflictingTxns)}");
                return Response.CreateWaiting(transactionId, $"Lock conflict - waiting for T{string.Join(",", conflictingTxns)}", obj, actionType);
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

    /// <summary>
    /// Mendeteksi deadlock menggunakan DFS pada Wait-For Graph.
    /// Cek apakah penambahan edge dari 'startTxnId' ke 'targetTxnId' akan membuat siklus.
    /// </summary>
    private bool DetectDeadlock(int startTxnId, int targetTxnId)
    {
        var visited = new HashSet<int>();
        var stack = new Stack<int>();

        // Mulai penelusuran dari target (orang yang kita tunggu).
        // Jika dari target bisa kembali ke startTxnId, berarti ada siklus.
        stack.Push(targetTxnId);

        while (stack.Count > 0)
        {
            var currentId = stack.Pop();

            if (currentId == startTxnId)
            {
                return true; // *Siklus ditemukan*: start -> ... -> target -> ... -> start
            }

            if (!visited.Contains(currentId))
            {
                visited.Add(currentId);

                if (_transactions.TryGetValue(currentId, out var currentState))
                {
                    // Semua transaksi yang sedang ditunggu currentId dimasukkan
                    foreach (var waitingId in currentState.WaitingFor)
                    {
                        stack.Push(waitingId);
                    }
                }
            }
        }

        return false;
    }


    public void LogObject(DatabaseObject obj, int transactionId)
    {
        var log = _objectAccessLog.GetOrAdd(transactionId, _ => new List<(DatabaseObject, DateTime)>());
        lock (log)
        {
            log.Add((obj, DateTime.UtcNow));
        }
        Console.WriteLine($"[2PL] Logged object access: T{transactionId} accessed {obj.ToQualifiedString()}");
    }

}
