namespace mDBMS.QueryProcessor.Contracts
{
    public interface IFailureRecovery
    {
        void WriteLog(ExecutionResult info);
    }
}
