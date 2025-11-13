using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.CLI.Mocks
{
    public class MockFailureRecovery : IFailureRecoveryManager
    {
        public void Recover(RecoverCriteria criteria)
        {
            Console.WriteLine($"[MOCK FRM]: Recover dipanggil.");
        }

        public void SaveCheckpoint()
        {
            Console.WriteLine($"[MOCK FRM]: SaveCheckpoint dipanggil.");
        }

        public void WriteLog(ExecutionResult info)
        {
            Console.WriteLine($"[MOCK FRM]: WriteLog dipanggil. Success={info.Success}, Message='{info.Message}'");
        }
    }
}
