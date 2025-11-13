using System;
using System.Collections.Concurrent;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Models.CCM;

namespace mDBMS.ConcurrencyControl;

/// <summary>
/// Kelas untuk fitur concurrency control
/// </summary>
public class ConcurrencyControlManager : IConcurrencyControlManager
{
    private int _nextTransactionId = 1;
    private readonly object _lockObject = new();

    // Menyimpan status transaksi untuk tracking
    private readonly ConcurrentDictionary<int, TransactionStatus> _transactions;

    public ConcurrencyControlManager()
    {
        _transactions = new ConcurrentDictionary<int, TransactionStatus>();
        Console.WriteLine("[CCM] ConcurrencyControlManager initialized (STUB MODE for Milestone 1)");
    }

    /// <summary>
    /// Memulai transaksi baru
    /// </summary>
    public int BeginTransaction()
    {
        lock (_lockObject)
        {
            int transactionId = _nextTransactionId++;
            _transactions.TryAdd(transactionId, TransactionStatus.Active);

            Console.WriteLine($"[STUB CCM]: BeginTransaction dipanggil. Transaction ID: {transactionId}");

            return transactionId;
        }
    }

    /// <summary>
    /// Memvalidasi apakah aksi pada objek diizinkan
    /// </summary>
    public Response ValidateObject(Common.Models.CCM.Action action)
    {
        Console.WriteLine($"[STUB CCM]: ValidateObject dipanggil.");
        Console.WriteLine($" - Transaction ID: {action.TransactionId}");
        Console.WriteLine($" - Action Type: {action.ActionType}");
        Console.WriteLine($" - Object: {action.DatabaseObject}");
        Console.WriteLine($" - Result: ALLOWED (stub always allows)");

        // TODO: ubah di next milestone (sekarang semua masih allowed)
        return Response.CreateAllowed(
            action.TransactionId,
            action.DatabaseObject,
            action.ActionType
        );
    }

    /// <summary>
    /// Mengakhiri transaksi
    /// </summary>
    public bool EndTransaction(int transactionId, bool commit)
    {
        Console.WriteLine($"[STUB CCM]: EndTransaction dipanggil untuk transaksi ID {transactionId}.");
        Console.WriteLine($" - Commit: {commit}");

        if (_transactions.TryGetValue(transactionId, out _))
        {
            var newStatus = commit ? TransactionStatus.Committed : TransactionStatus.Aborted;
            _transactions[transactionId] = newStatus;

            Console.WriteLine($" - Status: Transaction {transactionId} {(commit ? "COMMITTED" : "ABORTED")}");
            return true;
        }

        Console.WriteLine($"Warning: Transaction {transactionId} not found");
        return false;
    }

    /// <summary>
    /// Abort transaksi
    /// </summary>
    public bool AbortTransaction(int transactionId)
    {
        Console.WriteLine($"[STUB CCM]: AbortTransaction dipanggil untuk transaksi ID {transactionId}.");
        return EndTransaction(transactionId, commit: false);
    }

    /// <summary>
    /// Commit transaksi
    /// </summary>
    public bool CommitTransaction(int transactionId)
    {
        Console.WriteLine($"[STUB CCM]: CommitTransaction dipanggil untuk transaksi ID {transactionId}.");
        return EndTransaction(transactionId, commit: true);
    }

    /// <summary>
    /// Mendapatkan status transaksi
    /// </summary>
    public TransactionStatus GetTransactionStatus(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var status))
        {
            return status;
        }
        return TransactionStatus.Aborted;
    }

    /// <summary>
    /// Memeriksa apakah transaksi aktif
    /// </summary>
    public bool IsTransactionActive(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var status))
        {
            return status == TransactionStatus.Active;
        }
        return false;
    }
}
