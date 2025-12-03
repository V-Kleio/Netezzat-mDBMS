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
        public int TransactionId { get; set; }
        public string? TableName { get; set; }
        public string? BeforeImage { get; set; }
        public string? AfterImage { get; set; }
        public string? RowIdentifier { get; set; }

        public ExecutionResult()
        {
            ExecutedAt = DateTime.Now;
        }
    }
}
