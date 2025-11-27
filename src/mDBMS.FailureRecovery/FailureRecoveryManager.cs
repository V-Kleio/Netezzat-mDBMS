using mDBMS.Common.Transaction;
using mDBMS.Common.Interfaces;
using System.Text;
using mDBMS.Common.Data;


// ini real
namespace mDBMS.FailureRecovery
{
    public class FailureRecoveryManager : IFailureRecoveryManager, IBufferManager
    {
        private readonly BufferPool _bufferPool;

        // Dependency untuk Query Processor (untuk recovery query)
        private IQueryProcessor? _queryProcessor;

        // Dependency untuk Storage Manager (untuk checkpoint flush)
        private IStorageManager? _storageManager;

        private readonly string _logFilePath;
        private readonly string _logDirectory;
        private long _currentLSN;
        private readonly object _logLock = new object();
        private byte[]? _buffer;

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

            // baca LSN terakhir dari file log jika ada
            if (File.Exists(_logFilePath))
            {
                _currentLSN = ReadLastLSN();
            }
        }

        // ======================================== MAIN ========================================
        public void WriteLog(ExecutionResult info)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }

             if (!info.Success)
            {
                return;
            }

            // operasi SELECT tidak perlu di-log karena tidak mengubah data
            if (info.Query.TrimStart().ToUpperInvariant().StartsWith("SELECT"))
            {
                return;
            }

            lock (_logLock)
            {
                try
                {
                    // tentukan tipe operasi dari query
                    var operationType = DetermineOperationType(info.Query);

                    // buat log entry berdasarkan tipe operasi
                    LogEntry logEntry = CreateLogEntryFromExecutionResult(info, operationType);

                    // serialize dan tulis ke file
                    string serializedLog = logEntry.Serialize();
                    File.AppendAllText(_logFilePath, serializedLog + Environment.NewLine);

                    // increment LSN untuk entry berikutnya
                    _currentLSN++;
                }
                catch (Exception ex)
                {
                    // log error dan re-throw karena write-ahead logging harus berhasil
                    Console.Error.WriteLine($"[ERROR FRM]: Gagal menulis log - {ex.Message}");
                    throw;
                }
            }
        }

        // Undo Wajib: undo transaksi yang di-abort (Transaction Abort Recovery)
        // Gunanya buat rollback transaksi yang gagal/dibatalin based on ID transaksi yang mau di-undo
        public bool UndoTransaction(int transactionId)
        {
            Console.WriteLine($"[FRM UNDO]: Memulai undo untuk Transaction T{transactionId}");

            if (_queryProcessor == null)
            {
                Console.Error.WriteLine("[ERROR FRM]: Query Processor belum diset. Undo tidak dapat dilakukan.");
                return false;
            }

            // No log (nothing to do -> success)
            if (!File.Exists(_logFilePath))
            {
                Console.WriteLine("[FRM UNDO]: File log tidak ditemukan. Tidak ada yang perlu di-undo.");
                return true;
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


        // Recover up until recover criteria, uses ExecuteRecoveryQuery
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
                    var remainingDirtyPages = _bufferPool.FlushAll();

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
                // TODO (SM): Integrate dengan Storage Manager
                return false;
            }

            try
            {
                // TODO (SM): Convert Page object ke DataWrite format yang sesuai untuk SM
                // Contoh implementasi:
                // var dataWrite = ConvertPageToDataWrite(page);
                // int result = _storageManager.WriteBlock(dataWrite);
                // return result > 0;

                // Sementara return false karena belum terintegrasi
                return false;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[FRM]: Gagal flush page {page.TableName}-{page.BlockID} - {ex.Message}");
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
                var result = _queryProcessor.ExecuteQuery(undoQuery);

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
            if (string.IsNullOrEmpty(entry.TableName) || string.IsNullOrEmpty(entry.RowIdentifier))
            {
                Console.Error.WriteLine($"[ERROR FRM]: Missing TableName or RowIdentifier untuk undo INSERT");
                return string.Empty;
            }

            // Assuming RowIdentifier contains primary key information
            // Format: DELETE FROM table WHERE primary_key = value
            return $"DELETE FROM {entry.TableName} WHERE {entry.RowIdentifier}";
        }

        /// Undo UPDATE = Restore BeforeImage
        private string GenerateUndoForUpdate(LogEntry entry)
        {
            if (string.IsNullOrEmpty(entry.TableName) ||
                string.IsNullOrEmpty(entry.RowIdentifier) ||
                string.IsNullOrEmpty(entry.BeforeImage) ||
                entry.BeforeImage == "NULL")
            {
                Console.Error.WriteLine($"[ERROR FRM]: Missing data untuk undo UPDATE");
                return string.Empty;
            }

            try
            {
                // Parse BeforeImage (format: {"col1":"val1","col2":"val2"})
                var beforeData = ParseRowData(entry.BeforeImage);

                if (beforeData.Count == 0)
                {
                    Console.Error.WriteLine($"[ERROR FRM]: Failed to parse BeforeImage");
                    return string.Empty;
                }

                // Build SET clause
                var setClause = string.Join(", ",
                    beforeData.Select(kv => $"{kv.Key} = '{EscapeSqlString(kv.Value)}'"));

                // Format: UPDATE table SET col1=val1, col2=val2 WHERE primary_key = value
                return $"UPDATE {entry.TableName} SET {setClause} WHERE {entry.RowIdentifier}";
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Exception generating undo UPDATE - {ex.Message}");
                return string.Empty;
            }
        }

        /// Undo DELETE = Re-insert BeforeImage
        private string GenerateUndoForDelete(LogEntry entry)
        {
            if (string.IsNullOrEmpty(entry.TableName) ||
                string.IsNullOrEmpty(entry.BeforeImage) ||
                entry.BeforeImage == "NULL")
            {
                Console.Error.WriteLine($"[ERROR FRM]: Missing data untuk undo DELETE");
                return string.Empty;
            }

            try
            {
                // Parse BeforeImage
                var beforeData = ParseRowData(entry.BeforeImage);

                if (beforeData.Count == 0)
                {
                    Console.Error.WriteLine($"[ERROR FRM]: Failed to parse BeforeImage");
                    return string.Empty;
                }

                // Build column names and values
                var columns = string.Join(", ", beforeData.Keys);
                var values = string.Join(", ",
                    beforeData.Values.Select(v => $"'{EscapeSqlString(v)}'"));

                // Format: INSERT INTO table (col1, col2) VALUES (val1, val2)
                return $"INSERT INTO {entry.TableName} ({columns}) VALUES ({values})";
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Exception generating undo DELETE - {ex.Message}");
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

            return entries;
        }

        /// <summary>
        /// tentukan tipe operasi dari query string
        /// </summary>
        private LogOperationType DetermineOperationType(string query)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                throw new ArgumentException("Query tidak boleh kosong");
            }

            var normalizedQuery = query.Trim().ToUpperInvariant();

            if (normalizedQuery.StartsWith("BEGIN"))
                return LogOperationType.BEGIN_TRANSACTION;

            if (normalizedQuery.StartsWith("COMMIT"))
                return LogOperationType.COMMIT;

            if (normalizedQuery.StartsWith("ROLLBACK") || normalizedQuery.StartsWith("ABORT"))
                return LogOperationType.ABORT;

            if (normalizedQuery.StartsWith("INSERT"))
                return LogOperationType.INSERT;

            if (normalizedQuery.StartsWith("UPDATE"))
                return LogOperationType.UPDATE;

            if (normalizedQuery.StartsWith("DELETE"))
                return LogOperationType.DELETE;

            // operasi SELECT tidak perlu di-log karena tidak mengubah data
            throw new InvalidOperationException($"Tipe operasi tidak dikenali atau tidak perlu di-log: {query}");
        }

        /// <summary>
        /// buat LogEntry dari ExecutionResult
        /// </summary>
        private LogEntry CreateLogEntryFromExecutionResult(ExecutionResult info, LogOperationType operationType)
        {
            // ambil TransactionId dari ExecutionResult
            int transactionId = info.TransactionId ?? -1;

            // ambil informasi dari ExecutionResult yang sudah diisi oleh Query Processor
            string? tableName = info.TableName;
            string? beforeImage = info.BeforeImage;
            string? afterImage = info.AfterImage;
            string? rowIdentifier = info.RowIdentifier;

            // fallback: jika tableName tidak ada, coba extract dari query
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = ExtractTableNameFromQuery(info.Query);
            }

            // fallback: jika afterImage tidak ada tapi ada Data (untuk SELECT yang di-log), serialize
            if (string.IsNullOrEmpty(afterImage) && info.Data != null && info.Data.Any())
            {
                var firstRow = info.Data.First();
                afterImage = SerializeRowData(firstRow);
            }

            // buat log entry sesuai tipe operasi
            return operationType switch
            {
                LogOperationType.BEGIN_TRANSACTION => LogEntry.CreateBeginTransaction(_currentLSN, transactionId),
                LogOperationType.COMMIT => LogEntry.CreateCommit(_currentLSN, transactionId),
                LogOperationType.ABORT => LogEntry.CreateAbort(_currentLSN, transactionId),
                LogOperationType.INSERT => LogEntry.CreateInsert(_currentLSN, transactionId, tableName ?? "UNKNOWN", rowIdentifier ?? "UNKNOWN", afterImage ?? "NULL"),
                LogOperationType.UPDATE => LogEntry.CreateUpdate(_currentLSN, transactionId, tableName ?? "UNKNOWN", rowIdentifier ?? "UNKNOWN", beforeImage ?? "NULL", afterImage ?? "NULL"),
                LogOperationType.DELETE => LogEntry.CreateDelete(_currentLSN, transactionId, tableName ?? "UNKNOWN", rowIdentifier ?? "UNKNOWN", beforeImage ?? "NULL"),
                _ => throw new InvalidOperationException($"Tipe operasi tidak didukung: {operationType}")
            };
        }

        /// <summary>
        /// ekstrak nama tabel dari query string (simple parser)
        /// </summary>
        private string? ExtractTableNameFromQuery(string query)
        {
            if (string.IsNullOrWhiteSpace(query))
                return null;

            var tokens = query.Trim().Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

            // INSERT INTO table_name ...
            if (tokens.Length > 2 && tokens[0].Equals("INSERT", StringComparison.OrdinalIgnoreCase)
                && tokens[1].Equals("INTO", StringComparison.OrdinalIgnoreCase))
            {
                return tokens[2];
            }

            // UPDATE table_name ...
            if (tokens.Length > 1 && tokens[0].Equals("UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                return tokens[1];
            }

            // DELETE FROM table_name ...
            if (tokens.Length > 2 && tokens[0].Equals("DELETE", StringComparison.OrdinalIgnoreCase)
                && tokens[1].Equals("FROM", StringComparison.OrdinalIgnoreCase))
            {
                return tokens[2];
            }

            return null;
        }

        /// <summary>
        /// serialize row data menjadi string format JSON-like
        /// </summary>
        private string SerializeRowData(mDBMS.Common.Data.Row row)
        {
            var sb = new StringBuilder();
            sb.Append("{");

            var columns = row.Columns.Select(kv => $"\"{kv.Key}\":\"{kv.Value}\"");
            sb.Append(string.Join(",", columns));

            sb.Append("}");
            return sb.ToString();
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
        /// Parse row data dari JSON-like format menjadi dictionary
        /// Format: {"col1":"val1","col2":"val2"}
        /// </summary>
        private Dictionary<string, string> ParseRowData(string jsonLike)
        {
            var result = new Dictionary<string, string>();

            if (string.IsNullOrEmpty(jsonLike))
                return result;

            try
            {
                var content = jsonLike.Trim().Trim('{', '}');
                if (string.IsNullOrEmpty(content))
                    return result;
                // Split by comma (simple parser, assumes no commas in values)
                var pairs = content.Split(',');

                foreach (var pair in pairs)
                {
                    // Split by colon
                    var parts = pair.Split(new[] { ':' }, 2);

                    if (parts.Length == 2)
                    {
                        // Remove quotes and whitespace
                        var key = parts[0].Trim().Trim('"');
                        var value = parts[1].Trim().Trim('"');
                        result[key] = value;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR FRM]: Failed to parse row data - {ex.Message}");
            }

            return result;
        }

        /// <summary>
        /// Escape single quotes in SQL strings
        /// </summary>
        private string EscapeSqlString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            return value.Replace("'", "''");
        }


        // ======================================== BUFFER MANAGER ========================================

        public byte[] ReadFromBuffer(string tableName, int blockId)
        {
            Page? page = _bufferPool.GetPage(tableName, blockId);

            if (page != null)
            {
                return page.Data;
            }

            return Array.Empty<byte>();
        }

        public void WriteToBuffer(Page page)
        {
            Page? evictedPage = _bufferPool.AddOrUpdatePage(page);

            if (evictedPage != null && evictedPage.IsDirty)
            {
                // Flush evicted dirty page to disk
                bool success = FlushPageToDisk(evictedPage);
                if (success)
                {
                    _bufferPool.MarkClean(evictedPage.TableName, evictedPage.BlockID);
                }
            }
        }

        public List<Page> GetDirtyPages()
        {
            return _bufferPool.GetDirtyPages();
        }

        public List<Page> FlushAll()
        {
            return _bufferPool.FlushAll();
        }

    }
}
