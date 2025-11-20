using mDBMS.Common.Data;

namespace mDBMS.Common.Transaction
{
    /// <summary>
    /// class untuk ExecutionResult
    /// class ini diimplementasiin yg CCM
    /// </summary>
    public class ExecutionResult
    {
        public string Query { get; set; }
        public bool Success { get; set; }
        public string Message { get; set; }
        public DateTime ExecutedAt { get; set; }
        public IEnumerable<Row>? Data { get; set; }

        // field tambahan untuk mendukung Failure Recovery Manager
        // diperlukan agar WriteLog dapat membuat log entry yang lengkap untuk UNDO/REDO
        public int? TransactionId { get; set; }           // ID transaksi dari CCM
        public string? TableName { get; set; }            // nama tabel yang terpengaruh
        public string? RowIdentifier { get; set; }        // primary key dari row yang terpengaruh
        public string? BeforeImage { get; set; }          // data sebelum perubahan (untuk UNDO)
        public string? AfterImage { get; set; }           // data setelah perubahan (untuk REDO)

        public ExecutionResult()
        {
            ExecutedAt = DateTime.Now;
        }
    }
}
