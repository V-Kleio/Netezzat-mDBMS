using System;

namespace mDBMS.Common.DTOs
{

    public class RecoverCriteria
    {
        public DateTime Timestamp { get; set; } 
        public int TransactionId { get; set; }
    }
}