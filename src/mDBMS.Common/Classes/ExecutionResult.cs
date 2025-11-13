using System;

namespace mDBMS.Common.DTOs
{
    /// <summary>
    /// stub class untuk ExecutionResult
    /// class ini diimplementasiin yg CCM
    /// </summary>
    public class ExecutionResult
    {
        public string Query { get; set; } = string.Empty;
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public DateTime ExecutedAt { get; set; }

        public ExecutionResult()
        {
            ExecutedAt = DateTime.Now;
        }
    }
}
