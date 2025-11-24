using mDBMS.Common.Transaction;
namespace mDBMS.Common.Interfaces;

public interface IQueryProcessor
{
    ExecutionResult ExecuteQuery(string query, int transactionId);
}