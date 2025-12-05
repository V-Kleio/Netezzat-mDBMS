using mDBMS.Common.Transaction;

namespace mDBMS.Common.Interfaces

{
    public interface IFailureRecoveryManager
	{
		//ExecutionResult blom ada classnya, bukan kita yang implement bikin dummy aja klao mo test , tanya klompok CCM
		void WriteLog(ExecutionLog info);

		void SaveCheckpoint();

		//RecoverCriteria udah ada di mDBMS.Common/DTOs
        void Recover(RecoverCriteria criteria);

		bool UndoTransaction(int transactionId);
    }
}
