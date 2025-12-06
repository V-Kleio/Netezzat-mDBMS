using mDBMS.Common.Transaction;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;


// ini real
namespace mDBMS.FailureRecovery
{
    public class FailureRecoveryManager : IFailureRecoveryManager, IBufferManager
    {
        private readonly BufferPool _bufferPool;

        private IQueryProcessor? _queryProcessor;
        private IStorageManager? _storageManager;

        private readonly string _logFilePath;
        private readonly string _logDirectory;
        private long _currentLSN;
        private readonly object _logLock = new object();

        private List<LogEntry> _logBuffer = new List<LogEntry>();

        // Constants for automatic flushing
        private const int MaxLogBufferSize = 100; // Flush log when buffer udah mo lewat size (100)
        private const int CheckpointInterval = 10; // Checkpoint every N (10) commits
        private int _commitsSinceLastCheckpoint = 0;

        public FailureRecoveryManager(IQueryProcessor? queryProcessor = null, IStorageManager? storageManager = null)
        {
            _bufferPool = new BufferPool();

            _queryProcessor = queryProcessor;
            _storageManager = storageManager;

            _logDirectory = Path.Combine(Directory.GetCurrentDirectory(), "logs");
            _logFilePath = Path.Combine(_logDirectory, "mDBMS.log");
            _currentLSN = 0;

            // buat direktori logs jika belum ada
            if (!Directory.Exists(_logDirectory))
            {
                Directory.CreateDirectory(_logDirectory);
            }

            // buat file log jika belum ada
            if (!File.Exists(_logFilePath))
            {
                File.Create(_logFilePath).Close();
            }

            // baca LSN terakhir dari file log jika ada
            _currentLSN = ReadLastLSN();
        }

        // ======================================== MAIN ========================================
        /// <summary>
        /// WriteLog untuk data operations (INSERT/UPDATE/DELETE) dari QP
        /// </summary>
        public void WriteLog(ExecutionLog info)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }


             lock (_logLock)
            {
                var entry = new LogEntry
                {
                    LSN = _currentLSN,
                    Timestamp = DateTime.Now,
                    TransactionId = info.TransactionId,
                    OperationType = MapOperationType(info.Operation),  // Convert ExecutionLog.OperationType → LogOperationType
                    TableName = info.TableName,
                    RowIdentifier = info.RowIdentifier,
                    BeforeImage = info.BeforeImage,
                    AfterImage = info.AfterImage
                };

                _logBuffer.Add(entry);
                _currentLSN++;

                // Auto-flush if log buffer is full
                if (_logBuffer.Count >= MaxLogBufferSize)
                {
                    FlushLogBuffer();
                }
            }
        }

        /// <summary>
        /// WriteLogEntry untuk transaction control (BEGIN/COMMIT/ABORT) dan CHECKPOINT
        /// Digunakan oleh CCM/TM atau internal FRM
        /// Trigger otomatis periodic checkpoint
        /// </summary>
        public void WriteLogEntry(LogEntry entry)
        {
            if (entry == null)
            {
                throw new ArgumentNullException(nameof(entry));
            }

            lock (_logLock)
            {
                // Set LSN jika belum di-set
                if (entry.LSN == 0 || entry.LSN < _currentLSN)
                {
                    entry.LSN = _currentLSN;
                }

                _logBuffer.Add(entry);
                _currentLSN++;

                // Flush on COMMIT
                if (entry.OperationType == LogOperationType.COMMIT)
                {
                    FlushLogBuffer();

                    // INI TRIGGER PERIODIC CHECKPOINT based on commit count
                    _commitsSinceLastCheckpoint++;
                    if (_commitsSinceLastCheckpoint >= CheckpointInterval)
                    {
                        Console.WriteLine($"[FRM]: Triggering periodic checkpoint after {_commitsSinceLastCheckpoint} commits");
                        SaveCheckpoint();
                        _commitsSinceLastCheckpoint = 0;
                    }
                }
                // Flush on ABORT, ensures abort is logged
                else if (entry.OperationType == LogOperationType.ABORT)
                {
                    FlushLogBuffer();
                }
                // Auto-flush if log buffer is full
                else if (_logBuffer.Count >= MaxLogBufferSize)
                {
                    FlushLogBuffer();
                }
            }
        }

        /// <summary>
        /// Expose current LSN untuk factory methods
        /// </summary>
        public long GetCurrentLSN()
        {
            return _currentLSN;
        }

        // Undo Wajib: undo transaksi yang di-abort (Transaction Abort Recovery)
        // ? paramnya bisa RecoverCriteria
        // Gunanya buat rollback transaksi yang gagal/dibatalin based on ID transaksi yang mau di-undo
        public bool UndoTransaction(int transactionId)
        {
            if (transactionId <= 0)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Invalid transaction ID: {transactionId}");
                return false;
            }

            Console.WriteLine($"[FRM UNDO]: Memulai undo untuk Transaction T{transactionId}");

            if (_queryProcessor == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Query Processor belum diset. Undo tidak dapat dilakukan.");
                return false;
            }

            try
            {
                // Baca semua log entries
                var logEntries = ReadAllLogEntries();

                if (logEntries.Count == 0)
                {
                    Console.WriteLine("[FRM UNDO]: Log kosong. Tidak ada yang perlu di-undo.");
                    return true;
                }

                // Filter log entries based on transaction id
                var transactionEntries = logEntries
                    .Where(entry => entry.TransactionId == transactionId)
                    .ToList();

                if (transactionEntries.Count == 0)
                {
                    Console.WriteLine($"[FRM UNDO]: Tidak ada log entries untuk Transaction T{transactionId}");
                    return true;
                }

                Console.WriteLine($"[FRM UNDO]: Ditemukan {transactionEntries.Count} log entries untuk T{transactionId}");

                // backward UNDO (dari operasi terakhir ke awal)
                int undoCount = 0;
                int failCount = 0;

                for (int i = transactionEntries.Count - 1; i >= 0; i--)
                {
                    var logEntry = transactionEntries[i];

                    // Skip BEGIN_TRANSACTION entry biar ga masuk txn lain (undo cuman dilakuin ke DML -> CUD)
                    if (logEntry.OperationType == LogOperationType.BEGIN_TRANSACTION)
                    {
                        Console.WriteLine($"[FRM UNDO]: Reached BEGIN_TRANSACTION for T{transactionId}");
                        break;
                    }

                    bool success = ExecuteRecoveryQuery(logEntry);

                    if (success) undoCount++;
                    else failCount++;

                }

                if (failCount > 0)
                {
                    Console.Error.WriteLine($"[FRM UNDO]: Undo selesai dengan {failCount} kegagalan. Total {undoCount} operasi berhasil di-undo.");
                    return false;
                }

                Console.WriteLine($"[FRM UNDO]: Undo berhasil! Total {undoCount} operasi di-undo untuk T{transactionId}");
                return true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Gagal melakukan undo - {ex.Message}");
                Console.Error.WriteLine($"[ERROR FRM]: Stack Trace: {ex.StackTrace}");
                return false;
            }
        }


        /// <summary>
        /// Flush all log entries in the log buffer to disk (.log)
        /// </summary>
        public void FlushLogBuffer()
        {
            List<LogEntry> entriesToFlush;

            lock (_logLock)
            {
                if (_logBuffer.Count == 0) return;

                entriesToFlush = [.. _logBuffer];
                _logBuffer.Clear();
            }

            try
            {
                var lines = entriesToFlush.Select(e => e.Serialize());
                File.AppendAllLines(_logFilePath, lines);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Failed to flush log buffer - {ex.Message}");

                lock (_logLock)
                {
                    _logBuffer.InsertRange(0, entriesToFlush);
                }
                throw;
            }
        }
        /// <summary>
        /// (INI BONUS, belom dipake dulu, need REDO) Recover up until recover criteria, uses ExecuteRecoveryQuery
        /// Recovery utama wajib yang dipake itu UndoTransaction (Abort)
        /// </summary>
        public void Recover(RecoverCriteria criteria)
        {
            Console.WriteLine($"[FRM RECOVER]: Memulai recovery untuk TransactionId={criteria.TransactionId}, Timestamp={criteria.Timestamp}");

            if (_queryProcessor == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Query Processor belum diset. Recovery tidak dapat dilakukan.");
                return;
            }

            if (!File.Exists(_logFilePath))
            {
                Console.WriteLine("[FRM RECOVER]: File log tidak ditemukan. Tidak ada yang perlu di-recover.");
                return;
            }

            try
            {
                // Baca semua log entries
                var logEntries = ReadAllLogEntries();

                if (logEntries.Count == 0)
                {
                    Console.WriteLine("[FRM RECOVER]: Log kosong. Tidak ada yang perlu di-recover.");
                    return;
                }

                Console.WriteLine($"[FRM RECOVER]: Ditemukan {logEntries.Count} log entries");

                // Proses recovery secara backward (dari entry terakhir)
                int recoveredCount = 0;
                for (int i = logEntries.Count - 1; i >= 0; i--)
                {
                    var logEntry = logEntries[i];

                    // Cek apakah entry ini memenuhi kriteria recovery
                    if (!ShouldRecoverEntry(logEntry, criteria))
                    {
                        // Jika kriteria tidak terpenuhi, hentikan recovery
                        Console.WriteLine($"[FRM RECOVER]: Recovery berhenti di LSN={logEntry.LSN} (kriteria tidak terpenuhi)");
                        break;
                    }

                    // Jalankan recovery query untuk entry ini
                    bool success = ExecuteRecoveryQuery(logEntry);

                    if (success)
                    {
                        recoveredCount++;
                        Console.WriteLine($"[FRM RECOVER]: Berhasil recover LSN={logEntry.LSN}, Op={logEntry.OperationType}");
                    }
                    else
                    {
                        Console.WriteLine($"[FRM RECOVER]: Gagal recover LSN={logEntry.LSN}, Op={logEntry.OperationType}");
                    }
                }

                Console.WriteLine($"[FRM RECOVER]: Recovery selesai. Total {recoveredCount} operasi di-recover.");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Gagal melakukan recovery - {ex.Message}");
                Console.Error.WriteLine($"[ERROR FRM]: Stack Trace: {ex.StackTrace}");
            }
        }

        /// <summary>
        /// Implementasi Checkpoint: Flush dirty pages dari buffer ke disk dan tulis checkpoint entry ke log
        /// Flownya:
        /// 1. ambil semua dirty pages dari buffer
        /// 2. apply setiap perubahan ke disk via Storage Manager
        /// 3. flush buffer (kosongkan)
        /// 4. tulis checkpoint entry ke log
        /// </summary>
        public void SaveCheckpoint()
        {
            Console.WriteLine("[FRM CHECKPOINT]: Memulai proses checkpoint");

            lock (_logLock)
            {
                try
                {
                    // 0. Flush log buffer terlebih dahulu
                    FlushLogBuffer();

                    // 1. Ambil semua dirty pages dari buffer
                    var dirtyPages = _bufferPool.GetDirtyPages();

                    if (dirtyPages.Count == 0)
                    {
                        Console.WriteLine("[FRM CHECKPOINT]: Tidak ada dirty pages untuk di-flush");
                    }
                    else
                    {
                        Console.WriteLine($"[FRM CHECKPOINT]: Ditemukan {dirtyPages.Count} dirty pages");

                        // 2. Apply setiap perubahan ke disk via Storage Manager
                        int successCount = 0;
                        int failCount = 0;

                        foreach (var page in dirtyPages)
                        {
                            bool success = FlushPageToDisk(page);

                            if (success)
                            {
                                successCount++;
                                _bufferPool.MarkClean(page.TableName, page.BlockID);
                            }
                            else
                            {
                                failCount++;
                            }
                        }

                        Console.WriteLine($"[FRM CHECKPOINT]: Flush selesai - Success: {successCount}, Failed: {failCount}");

                        if (failCount > 0)
                        {
                            Console.Error.WriteLine($"[ERROR FRM]: Checkpoint tidak lengkap. {failCount} pages gagal di-flush");
                        }
                    }

                    // 3. Flush buffer (kosongkan)
                    var remainingDirtyPages = _bufferPool.FlushDirties();

                    if (remainingDirtyPages.Count > 0)
                    {
                        Console.WriteLine($"[FRM CHECKPOINT]: Buffer di-flush, sisa {remainingDirtyPages.Count} dirty pages");
                    }

                    // 4. Tulis checkpoint entry ke log
                    // Ambil daftar transaksi aktif (dari log entries yang belum COMMIT/ABORT)
                    var activeTransactions = GetActiveTransactions();

                    var checkpointEntry = LogEntry.CreateCheckpoint(_currentLSN, activeTransactions);
                    string serializedLog = checkpointEntry.Serialize();

                    File.AppendAllText(_logFilePath, serializedLog + Environment.NewLine);

                    Console.WriteLine($"[FRM CHECKPOINT]: Checkpoint entry ditulis ke log (LSN={_currentLSN}, Active Txns={activeTransactions.Count})");

                    // Increment LSN
                    _currentLSN++;

                    Console.WriteLine("[FRM CHECKPOINT]: Checkpoint selesai!");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[ERROR FRM]: Gagal melakukan checkpoint - {ex.Message}");
                    Console.Error.WriteLine($"[ERROR FRM]: Stack Trace: {ex.StackTrace}");
                    throw;
                }
            }
        }


        /// <summary>
        /// Flush page ke disk via Storage Manager
        /// </summary>
        private bool FlushPageToDisk(Page page)
        {
            if (_storageManager == null)
            {
                return false;
            }

            try
            {
                return _storageManager.WriteDisk(page) == 1;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Gagal flush page {page.TableName}-{page.BlockID} - {ex.Message}");
                return false;
            }
        }



        /// <summary>
        /// Ambil daftar transaksi yang masih aktif (belum COMMIT/ABORT)
        /// </summary>
        private List<int> GetActiveTransactions()
        {
            var activeTransactions = new HashSet<int>();

            try
            {
                if (!File.Exists(_logFilePath))
                    return new List<int>();

                var logEntries = ReadAllLogEntries();

                foreach (var entry in logEntries)
                {
                    if (entry.OperationType == LogOperationType.BEGIN_TRANSACTION)
                    {
                        activeTransactions.Add(entry.TransactionId);
                    }
                    else if (entry.OperationType == LogOperationType.COMMIT ||
                             entry.OperationType == LogOperationType.ABORT)
                    {
                        activeTransactions.Remove(entry.TransactionId);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Gagal membaca active transactions - {ex.Message}");
            }

            return activeTransactions.ToList();
        }


        // ======================================== Recovery Pipeline ========================================

        /// <summary>
        /// Dari Log Entry -> Undo Query, make/lewat QP
        /// </summary>
        /// <param name="entry"></param>
        /// <returns>true if successful, false otherwise</returns>
        private bool ExecuteRecoveryQuery(LogEntry entry)
        {
            Console.WriteLine($"[FRM UNDO]: Menjalankan recovery untuk LSN={entry.LSN}, Op={entry.OperationType}");

            if (_queryProcessor == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Query Processor tidak tersedia untuk menjalankan recovery query");
                return false;
            }

            try
            {
                // Skip transaction control operations (BEGIN, COMMIT, ABORT)
                if (entry.OperationType == LogOperationType.BEGIN_TRANSACTION ||
                    entry.OperationType == LogOperationType.COMMIT ||
                    entry.OperationType == LogOperationType.ABORT)
                {
                    Console.WriteLine($"[FRM UNDO]: Skipping {entry.OperationType} - tidak perlu di-undo");
                    return true;
                }

                // Generate undo SQL based on operation type
                string undoQuery = GenerateUndoQuery(entry);

                if (string.IsNullOrEmpty(undoQuery))
                {
                    Console.WriteLine($"[FRM UNDO]: Tidak ada undo query untuk LSN={entry.LSN}");
                    return true;
                }

                Console.WriteLine($"[FRM UNDO]: Executing: {undoQuery}");

                // Execute undo query via Query Processor
                var result = _queryProcessor.ExecuteQuery(undoQuery, -1);

                if (result.Success)
                {
                    Console.WriteLine($"[FRM UNDO]: Berhasil undo LSN={entry.LSN}");
                    return true;
                }
                else
                {
                    Console.Error.WriteLine($"[FRM UNDO]: Gagal undo LSN={entry.LSN} - {result.Message}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Exception saat execute recovery query - {ex.Message}");
                return false;
            }
        }

        private string GenerateUndoQuery(LogEntry entry)
        {
            switch (entry.OperationType)
            {
                case LogOperationType.INSERT:
                    // Undo INSERT: DELETE row yang di-insert
                    return GenerateUndoForInsert(entry);

                case LogOperationType.UPDATE:
                    // Undo UPDATE: Restore BeforeImage
                    return GenerateUndoForUpdate(entry);

                case LogOperationType.DELETE:
                    // Undo DELETE: Re-insert BeforeImage
                    return GenerateUndoForDelete(entry);

                default:
                    return string.Empty;
            }
        }

        // Undo INSERT = DELETE by RowIdentifier
        private string GenerateUndoForInsert(LogEntry entry)
        {
            if (string.IsNullOrEmpty(entry.TableName) || entry.AfterImage == null)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Missing TableName or AfterImage untuk undo INSERT");
                return string.Empty;
            }

            var whereClause = string.Join(" AND ",
                entry.AfterImage.Columns.Select(
                    kv => $"{kv.Key} = {(kv.Value is string ? "'" + EscapeSqlString(kv.Value?.ToString()?? "NULL") + "'" : kv.Value)}"
                )
            );

            // Assuming RowIdentifier contains primary key information
            // Format: DELETE FROM table WHERE primary_key = value
            return $"DELETE FROM {entry.TableName} WHERE {whereClause}";
        }

            /// Undo UPDATE = Restore BeforeImage
        private string GenerateUndoForUpdate(LogEntry entry)
        {
            if (entry.TableName == null ||
                entry.RowIdentifier == null ||
                entry.BeforeImage == null ||
                entry.AfterImage == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Missing data untuk undo UPDATE");
                return string.Empty;
            }

            try
            {
                // BeforeImage sudah Row, jadi akses langsung:
                var setClause = string.Join(", ",
                    entry.BeforeImage.Columns.Select(
                        kv => $"{kv.Key} = {(kv.Value is string ? "'" + EscapeSqlString(kv.Value?.ToString()?? "NULL") + "'" : kv.Value)}"
                    )
                );

                var whereClause = string.Join(" AND ",
                    entry.AfterImage.Columns.Select(
                        kv => $"{kv.Key} = {(kv.Value is string ? "'" + EscapeSqlString(kv.Value?.ToString()?? "NULL") + "'" : kv.Value)}"
                    )
                );

                return $"UPDATE {entry.TableName} SET {setClause} WHERE {whereClause}";
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Exception undo UPDATE - {ex.Message}");
                return string.Empty;
            }
        }



        /// Undo DELETE = Re-insert BeforeImage
        private string GenerateUndoForDelete(LogEntry entry)
        {
            if (entry.TableName == null ||
                entry.BeforeImage == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Missing data untuk undo DELETE");
                return string.Empty;
            }

            try
            {
                // Akses dari Row.Columns
                var columns = string.Join(", ", entry.BeforeImage.Columns.Keys);

                var values = string.Join(", ",
                    entry.BeforeImage.Columns.Values.Select(
                        v => $"'{EscapeSqlString(v?.ToString() ?? "NULL")}'"
                    )
                );

                return $"INSERT INTO {entry.TableName} ({columns}) VALUES ({values})";
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Exception undo DELETE - {ex.Message}");
                return string.Empty;
            }
        }


        // ======================================== Recovery Helper ========================================
        private bool ShouldRecoverEntry(LogEntry entry, RecoverCriteria criteria)
        {
            // Jika TransactionId diset (bukan 0 atau -1), check by TransactionId
            if (criteria.TransactionId > 0)
            {
                // Recover semua operasi dari transaksi ini
                return entry.TransactionId == criteria.TransactionId;
            }

            // Jika Timestamp diset, check by Timestamp
            if (criteria.Timestamp != DateTime.MinValue)
            {
                // Recover semua operasi setelah timestamp ini
                return entry.Timestamp >= criteria.Timestamp;
            }

            // Default: tidak recover
            return false;
        }

        // ======================================== WAL Management ========================================
        // Baca semua log entries dari file
        private List<LogEntry> ReadAllLogEntries()
        {
            var entries = new List<LogEntry>();

            try
            {
                var lines = File.ReadAllLines(_logFilePath);

                foreach (var line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    try
                    {
                        var entry = LogEntry.Deserialize(line);
                        entries.Add(entry);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"[WARNING FRM]: Gagal parse log entry - {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Gagal membaca file log - {ex.Message}");
                throw;
            }

            entries.AddRange(_logBuffer);

            return entries;
        }


        /// <summary>
        /// Map dari ExecutionLog.OperationType (enum dari QP) ke LogOperationType (enum di FRM)
        /// </summary>
        private LogOperationType MapOperationType(ExecutionLog.OperationType operation)
        {
            return operation switch
            {
                ExecutionLog.OperationType.INSERT => LogOperationType.INSERT,
                ExecutionLog.OperationType.UPDATE => LogOperationType.UPDATE,
                ExecutionLog.OperationType.DELETE => LogOperationType.DELETE,
                _ => throw new ArgumentException($"Unknown operation type: {operation}", nameof(operation))
            };
        }




        /// <summary>
        /// baca LSN terakhir dari file log
        /// </summary>
        private long ReadLastLSN()
        {
            try
            {
                if (!File.Exists(_logFilePath))
                    return 0;

                var lines = File.ReadAllLines(_logFilePath);
                if (lines.Length == 0)
                    return 0;

                // baca baris terakhir dan parse LSN
                var lastLine = lines[^1];
                var logEntry = LogEntry.Deserialize(lastLine);
                return logEntry.LSN + 1; // LSN berikutnya
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[WARNING FRM]: Gagal membaca LSN terakhir - {ex.Message}. Mulai dari 0.");
                return 0;
            }
        }

        // ======================================== PARSER Before/AfterImage Helpers ========================================


        /// <summary>
        /// Escape single quotes in SQL strings
        /// </summary>
        private string EscapeSqlString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return "NULL";

            return value.Replace("'", "''");
        }


        // ======================================== BUFFER MANAGER ========================================

        /// <summary>
        /// Read page from buffer. If page exists in buffer, return the page data.
        /// Otherwise, return an empty array.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="blockId">Block ID of the page</param>
        /// <returns>Page data from buffer, or an empty array if not found</returns>
        public byte[] ReadFromBuffer(string tableName, int blockId)
        {
            Page? page = _bufferPool.GetPage(tableName, blockId);

            if (page != null)
            {
                return page.Data;
            }

            return Array.Empty<byte>();
        }

        /// <summary>
        /// Write page to buffer pool. If the page is dirty, and the buffer pool is full,
        /// evict the page and flush it to disk (LRU eviction).
        /// </summary>
        public void WriteToBuffer(Page page)
        {
            Page? evictedPage = _bufferPool.AddOrUpdatePage(page);

            // Auto-flush evicted dirty pages when buffer is full (LRU eviction)
            if (evictedPage != null && evictedPage.IsDirty)
            {
                Console.WriteLine($"[FRM BUFFER]: Evicting page {evictedPage.TableName}-{evictedPage.BlockID}, flushing to disk");
                bool success = FlushPageToDisk(evictedPage);
                if (success)
                {
                    _bufferPool.MarkClean(evictedPage.TableName, evictedPage.BlockID);
                    Console.WriteLine($"[FRM BUFFER]: Page flushed successfully");
                }
                else
                {
                    Console.Error.WriteLine($"[FRM BUFFER]: Failed to flush evicted page!");
                }
            }
        }

        public List<Page> GetDirtyPages()
        {
            return _bufferPool.GetDirtyPages();
        }


        /// <summary>
        /// Flush all dirty pages from buffer pool to disk.
        /// </summary>
        /// <returns>List of flushed pages</returns>
        public List<Page> FlushAll()
        {
            List<Page> buffers = _bufferPool.FlushDirties();
            foreach (var page in buffers)
            {
                FlushPageToDisk(page);
            }
            return buffers;
        }

         private LogOperationType ParseOperationType(string operation)
        {
            if (string.IsNullOrWhiteSpace(operation))
            {
                throw new ArgumentException("Operation type cannot be null or empty", nameof(operation));
            }

            return operation.ToUpperInvariant() switch
            {
                "INSERT" => LogOperationType.INSERT,
                "UPDATE" => LogOperationType.UPDATE,
                "DELETE" => LogOperationType.DELETE,
                "BEGIN" => LogOperationType.BEGIN_TRANSACTION,
                "COMMIT" => LogOperationType.COMMIT,
                "ROLLBACK" => LogOperationType.ABORT,
                "ABORT" => LogOperationType.ABORT,

                // Alias/alternatif
                "BEGIN TRANSACTION" => LogOperationType.BEGIN_TRANSACTION,
                "COMMIT TRANSACTION" => LogOperationType.COMMIT,
                "ROLLBACK TRANSACTION" => LogOperationType.ABORT,

                // Checkpoint
                "CHECKPOINT" => LogOperationType.CHECKPOINT,
                "END CHECKPOINT" => LogOperationType.END_CHECKPOINT,
                "END_CHECKPOINT" => LogOperationType.END_CHECKPOINT,

                _ => throw new ArgumentException($"Unknown operation type: {operation}", nameof(operation))
            };
        }
    }
}
