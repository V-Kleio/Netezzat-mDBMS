using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessorDriver.Mocks;

public class MockFailureRecoveryManager : IFailureRecoveryManager
{
    private readonly TestObserver _observer;

    public MockFailureRecoveryManager(TestObserver observer)
    {
        _observer = observer;
    }

    public void WriteLog(ExecutionResult result)
    {
        _observer.Record($"FailureRecoveryManager.WriteLog(Query='{result.Query}', Success={result.Success})");
    }

    public void SaveCheckpoint()
    {
        _observer.Record("FailureRecoveryManager.SaveCheckpoint");
    }

    public bool UndoTransaction(int transactionId)
    {
        _observer.Record($"FailureRecoveryManager.UndoTransaction(ID={transactionId})");
        return true;
    }

    public void WriteLog(mDBMS.Common.Transaction.ExecutionLog log)
    {
        _observer.Record($"FailureRecoveryManager.WriteLog");
    }

    public void WriteLogEntry(LogEntry log)
    {
        _observer.Record($"FailureRecoveryManager.WriteLog");
    }

    public void Recover(RecoverCriteria criteria)
    {
        _observer.Record("FailureRecoveryManager.Recover");
    }
}
