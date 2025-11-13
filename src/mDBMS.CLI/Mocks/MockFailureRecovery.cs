using System;
using mDBMS.QueryProcessor.Contracts;

namespace mDBMS.CLI.Mocks
{
    public class MockFailureRecovery : IFailureRecovery
    {
        public void WriteLog(ExecutionResult info)
        {
            Console.WriteLine($"[MOCK FRM]: WriteLog dipanggil. Success={info.Success}, Message='{info.Message}'");
        }
    }
}
