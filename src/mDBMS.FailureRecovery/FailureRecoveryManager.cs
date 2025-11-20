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

        // DI later
        // private readonly IStorageManager _storageManager;

        private readonly string _logFilePath;
        private readonly string _logDirectory;
        private long _currentLSN;
        private readonly object _logLock = new object();
        private byte[] _buffer;

        public FailureRecoveryManager()
        {
            _bufferPool = new BufferPool();
            // _storageManager = storageManager;

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

        public void WriteLog(ExecutionResult info)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
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


        public void Recover(RecoverCriteria criteria)
        {
            Console.WriteLine($"[STUB FRM]: Recover dipanggil untuk TransactionId '{criteria.TransactionId}' pada Timestamp '{criteria.Timestamp}'");
        }

        public void SaveCheckpoint()
        {
            Console.WriteLine("[STUB FRM]: SaveCheckpoint dipanggil");
        }

        public byte[] ReadFromBuffer(string tableName, int blockId)
        {
            Console.WriteLine($"[FRM-BUFFER]: ReadFromBuffer dipanggil, Table={tableName}, BlockId={blockId}");            
            Page? page =_bufferPool.GetPage(tableName, blockId);

            if (page != null)
            {
                return page.Data;
            }

            return Array.Empty<byte>();
        }

        public void WriteToBuffer(Page page)
        {
            Console.WriteLine($"[FRM-BUFFER]: WriteToBuffer dipanggil, Table={page.TableName}, BlockId={page.BlockID}, IsDirty={page.IsDirty}");
            
            Page? evictedPage = _bufferPool.AddOrUpdatePage(page);

            if (evictedPage != null) 
            {
                if (evictedPage.IsDirty) 
                {
                    Console.WriteLine($"[FRM-BUFFER]: Eviction - Flushing dirty page {evictedPage.BlockID} of {evictedPage.TableName}");
                    FlushPage(evictedPage);
                }
            }
        }

        private void FlushPage (Page page) 
        {
            // TODO: Di sini  memanggil IStorageManager (sm.WriteBlock)
            // Namun, karena WriteBlock SM menerima DataWrite, perlu helper/wrapper
            // untuk mengubah Page object menjadi raw bytes yang siap ditulis ke disk.
            
            Console.WriteLine($"[FRM-FLUSH]: Memulai I/O Tulis ke disk untuk {page.TableName}-{page.BlockID}");

            // Implementasi stub sementara:
            // if (_storageManager != null) {
            //     _storageManager.WriteBlock(logic konversi Page ke DataWrite/byte[]);
            // }

            // Setelah sukses ditulis ke disk, halaman menjadi bersih lagi.
            page.IsDirty = false;
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
    }
}
