using mDBMS.Common.Transaction;
namespace mDBMS.QueryProcessor;

internal interface IQueryHandler
{
    public ExecutionResult HandleQuery(string query);
}