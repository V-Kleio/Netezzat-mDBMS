using mDBMS.Common.Data;

namespace mDBMS.Common.Transaction
{
    /// <summary>
    /// class untuk ExecutionResult
    /// class ini diimplementasiin yg CCM
    /// </summary>
    public class ExecutionLog
    {
        public OperationType Operation { get; set; }
        public int TransactionId { get; set; }           // ID transaksi dari CCM
        public string TableName { get; set; }            // nama tabel yang terpengaruh
        public string RowIdentifier { get; set; }        // primary key dari row yang terpengaruh
        public Row? BeforeImage { get; set; }             // data sebelum perubahan (untuk UNDO)
        public Row? AfterImage { get; set; }              // data setelah perubahan (untuk REDO)

        public enum OperationType
        {
            BEGIN,
            UPDATE,
            INSERT,
            DELETE,
            COMMIT,
            ABORT
        }
    }
}
