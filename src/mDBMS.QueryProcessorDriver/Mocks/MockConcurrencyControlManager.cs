using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessorDriver.Mocks;

public class MockConcurrencyControlManager : IConcurrencyControlManager
{
    private readonly TestObserver _observer;
    private int _nextTransactionId = 1;

    public MockConcurrencyControlManager(TestObserver observer)
    {
        _observer = observer;
    }

    public int BeginTransaction()
    {
        _observer.Record("ConcurrencyControlManager.BeginTransaction");
        return _nextTransactionId++;
    }

    public bool EndTransaction(int transactionId, bool commit)
    {
        _observer.Record($"ConcurrencyControlManager.EndTransaction(ID={transactionId}, Commit={commit})");
        return true;
    }
    
    public Response ValidateAction(mDBMS.Common.Transaction.Action action)
    {
        _observer.Record("ConcurrencyControlManager.ValidateAction");
        return new Response { Allowed = true, TransactionId = action.TransactionId };
    }

    public Response ValidateObject(mDBMS.Common.Transaction.Action action) 
    { 
        _observer.Record("ConcurrencyControlManager.ValidateObject"); 
        return new Response { Allowed = true }; 
    }

    public TransactionStatus GetTransactionStatus(int transactionId) 
    { 
        _observer.Record("ConcurrencyControlManager.GetTransactionStatus"); 
        return TransactionStatus.Active; 
    }

    public bool IsTransactionActive(int transactionId) 
    { 
        _observer.Record("ConcurrencyControlManager.IsTransactionActive"); 
        return true; 
    }

    public bool AbortTransaction(int transactionId) 
    { 
        _observer.Record("ConcurrencyControlManager.AbortTransaction"); 
        return true;
    }

    public bool CommitTransaction(int transactionId) 
    { 
        _observer.Record("ConcurrencyControlManager.CommitTransaction"); 
        return true;
    }

    public void LogObject(DatabaseObject obj, int transactionId) 
    { 
        _observer.Record("ConcurrencyControlManager.LogObject"); 
    }
}
