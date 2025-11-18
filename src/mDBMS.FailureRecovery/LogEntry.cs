namespace mDBMS.FailureRecovery
{
    /// Enum untuk tipe operasi dalam log
    public enum LogOperationType
    {
        BEGIN_TRANSACTION,
        COMMIT,
        ABORT,
        INSERT,
        UPDATE,
        DELETE,
        CHECKPOINT,
        END_CHECKPOINT
    }

    /// Class untuk merepresentasikan satu entry dalam log
    /// Format: [LogSequenceNumber]|[Timestamp]|[TransactionId]|[OperationType]|[TableName]|[BeforeImage]|[AfterImage]
    public class LogEntry
    {
        // Log Sequence Number - unique identifier untuk setiap log entry
        public long LSN { get; set; }
        
        // Timestamp kapan operasi dilakukan
        public DateTime Timestamp { get; set; }
        
        // Transaction ID yang melakukan operasi
        public int TransactionId { get; set; }
        
        // Tipe operasi (BEGIN, COMMIT, INSERT, UPDATE, etc.)
        public LogOperationType OperationType { get; set; }
        
        // Nama tabel yang terpengaruh (null untuk BEGIN/COMMIT/CHECKPOINT)
        public string? TableName { get; set; }
        
        // Before Image - data sebelum perubahan (untuk UNDO)
        public string? BeforeImage { get; set; }
        
        // After Image - data setelah perubahan (untuk REDO)
        public string? AfterImage { get; set; }
        
        // Primary Key atau Row ID yang terpengaruh
        public string? RowIdentifier { get; set; }

        /// Serialize log entry ke format string untuk disimpan ke file
        /// Format: LSN|Timestamp|TxnId|OpType|Table|RowId|BeforeImage|AfterImage
        public string Serialize()
        {
            var parts = new List<string>
            {
                LSN.ToString(),
                Timestamp.ToString("yyyy-MM-dd HH:mm:ss.ffffff"),
                TransactionId.ToString(),
                OperationType.ToString(),
                TableName ?? "NULL",
                RowIdentifier ?? "NULL",
                EscapeDelimiters(BeforeImage ?? "NULL"),
                EscapeDelimiters(AfterImage ?? "NULL")
            };
            
            return string.Join("|", parts);
        }

        /// Deserialize string menjadi LogEntry object
        public static LogEntry Deserialize(string logLine)
        {
            var parts = logLine.Split('|');
            
            if (parts.Length < 8)
            {
                throw new FormatException($"Invalid log format. Expected 8 parts, got {parts.Length}");
            }

            return new LogEntry
            {
                LSN = long.Parse(parts[0]),
                Timestamp = DateTime.Parse(parts[1]),
                TransactionId = int.Parse(parts[2]),
                OperationType = Enum.Parse<LogOperationType>(parts[3]),
                TableName = parts[4] == "NULL" ? null : parts[4],
                RowIdentifier = parts[5] == "NULL" ? null : parts[5],
                BeforeImage = UnescapeDelimiters(parts[6] == "NULL" ? null : parts[6]),
                AfterImage = UnescapeDelimiters(parts[7] == "NULL" ? null : parts[7])
            };
        }

        /// Escape delimiter characters dalam data
        private string EscapeDelimiters(string? data)
        {
            if (string.IsNullOrEmpty(data)) return "NULL";
            return data.Replace("|", "\\|").Replace("\n", "\\n").Replace("\r", "\\r");
        }

        /// Unescape delimiter characters
        private static string? UnescapeDelimiters(string? data)
        {
            if (string.IsNullOrEmpty(data)) return null;
            return data.Replace("\\|", "|").Replace("\\n", "\n").Replace("\\r", "\r");
        }

        /// Create log entry untuk BEGIN TRANSACTION
        public static LogEntry CreateBeginTransaction(long lsn, int transactionId)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.BEGIN_TRANSACTION,
                TableName = null,
                BeforeImage = null,
                AfterImage = null,
                RowIdentifier = null
            };
        }

        /// Create log entry untuk COMMIT
        public static LogEntry CreateCommit(long lsn, int transactionId)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.COMMIT,
                TableName = null,
                BeforeImage = null,
                AfterImage = null,
                RowIdentifier = null
            };
        }

        /// Create log entry untuk ABORT
        public static LogEntry CreateAbort(long lsn, int transactionId)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.ABORT,
                TableName = null,
                BeforeImage = null,
                AfterImage = null,
                RowIdentifier = null
            };
        }

        /// Create log entry untuk UPDATE
        public static LogEntry CreateUpdate(long lsn, int transactionId, string tableName, 
            string rowId, string beforeImage, string afterImage)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.UPDATE,
                TableName = tableName,
                RowIdentifier = rowId,
                BeforeImage = beforeImage,
                AfterImage = afterImage
            };
        }

        /// Create log entry untuk INSERT
        public static LogEntry CreateInsert(long lsn, int transactionId, string tableName, 
            string rowId, string afterImage)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.INSERT,
                TableName = tableName,
                RowIdentifier = rowId,
                BeforeImage = null, // INSERT tidak punya before image
                AfterImage = afterImage
            };
        }

        /// Create log entry untuk DELETE
        public static LogEntry CreateDelete(long lsn, int transactionId, string tableName, 
            string rowId, string beforeImage)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = transactionId,
                OperationType = LogOperationType.DELETE,
                TableName = tableName,
                RowIdentifier = rowId,
                BeforeImage = beforeImage,
                AfterImage = null // DELETE tidak punya after image
            };
        }

        /// Create log entry untuk CHECKPOINT
        public static LogEntry CreateCheckpoint(long lsn, List<int> activeTransactions)
        {
            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = -1, // Checkpoint tidak terkait dengan transaksi tertentu
                OperationType = LogOperationType.CHECKPOINT,
                TableName = null,
                BeforeImage = null,
                AfterImage = string.Join(",", activeTransactions) // Simpan daftar transaksi aktif
            };
        }

        public override string ToString()
        {
            return $"[LSN={LSN}] [{Timestamp:yyyy-MM-dd HH:mm:ss}] [Txn={TransactionId}] " +
                   $"[Op={OperationType}] [Table={TableName ?? "N/A"}] [Row={RowIdentifier ?? "N/A"}]";
        }
    }
}