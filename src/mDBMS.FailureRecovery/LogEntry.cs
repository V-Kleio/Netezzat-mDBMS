using mDBMS.Common.Data;

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
        public Row? BeforeImage { get; set; }

        // After Image - data setelah perubahan (untuk REDO)
        public Row? AfterImage { get; set; }

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
                EscapeRow(BeforeImage),
                EscapeRow(AfterImage)
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
                  BeforeImage = ParseRow(Unescape(parts[6])),
                AfterImage  = ParseRow(Unescape(parts[7]))
            };
        }

        // =======================================================
        // ESCAPE / UNESCAPE
        // =======================================================

        private static string EscapeRow(Row? row)
        {
            if (row == null)
                return "NULL";

            string json = SerializeRow(row);

            return json
                .Replace("\\", "\\\\")  // escape slash dulu
                .Replace("|", "\\|")
                .Replace("\n", "\\n")
                .Replace("\r", "\\r");
        }

        private static string SerializeRow(Row row)
        {
            var cols = row.Columns.Select(kv => $"\"{kv.Key}\":\"{kv.Value}\"");
            return "{" + string.Join(",", cols) + "}";
        }

        private static string? Unescape(string? data)
        {
            if (string.IsNullOrEmpty(data) || data == "NULL")
                return null;

            return data
                .Replace("\\n", "\n")
                .Replace("\\r", "\r")
                .Replace("\\|", "|")
                .Replace("\\\\", "\\"); // unescape slash terakhir
        }

        private static Row? ParseRow(string? json)
        {
            if (json == null)
                return null;

            var row = new Row();
            var body = json.Trim().Trim('{', '}');
            if (string.IsNullOrWhiteSpace(body))
                return row;

            var pairs = body.Split(',');

            foreach (var pair in pairs)
            {
                var kv = pair.Split(':', 2);
                var key = kv[0].Trim('"');
                var value = kv[1].Trim('"');
                row.Columns[key] = value;
            }

            return row;
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
            string rowId, Row beforeImage, Row afterImage)
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
            string rowId, Row afterImage)
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
            string rowId, Row beforeImage)
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
            // Simpan list txn aktif ke AfterImage dalam satu kolom
            var row = new Row();
            row.Columns["ActiveTransactions"] = string.Join(",", activeTransactions);

            return new LogEntry
            {
                LSN = lsn,
                Timestamp = DateTime.Now,
                TransactionId = -1,
                OperationType = LogOperationType.CHECKPOINT,
                TableName = null,
                BeforeImage = null,
                AfterImage = row,
                RowIdentifier = null
            };
        }

        public override string ToString()
        {
            return $"[LSN={LSN}] [{Timestamp:yyyy-MM-dd HH:mm:ss}] [Txn={TransactionId}] " +
                   $"[Op={OperationType}] [Table={TableName ?? "N/A"}] [Row={RowIdentifier ?? "N/A"}]";
        }
    }
}
