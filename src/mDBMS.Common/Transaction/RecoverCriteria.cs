namespace mDBMS.Common.Transaction
{

    public class RecoverCriteria
    {
        public DateTime Timestamp { get; set; } 
        public int TransactionId { get; set; }
    }
}