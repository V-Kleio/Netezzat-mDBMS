using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    private IStorageManager storageManager;
    private IConcurrencyControlManager concurrencyControlManager;
    private IFailureRecoveryManager failureRecoveryManager;
    private int transactionId;

    public Operator(
        IStorageManager storageManager,
        IFailureRecoveryManager failureRecoveryManager,
        IConcurrencyControlManager concurrencyControlManager,
        int transactionId
    ) {
        this.storageManager = storageManager;
        this.failureRecoveryManager = failureRecoveryManager;
        this.concurrencyControlManager = concurrencyControlManager;
        this.transactionId = transactionId;
    }
}