namespace mDBMS.Common.Interfaces;

public interface IQueryProcessor
{
    ExecutionResult ExecuteQuery(string? query);
}