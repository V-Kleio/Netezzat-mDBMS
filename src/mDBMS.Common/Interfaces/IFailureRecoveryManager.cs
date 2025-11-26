using mDBMS.Common.Transaction;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.Common.Interfaces

{
    public interface IFailureRecoveryManager
	{
		void WriteLog(ExecutionResult info);

		void SaveCheckpoint();

		//RecoverCriteria udah ada di mDBMS.Common/DTOs
        void Recover(RecoverCriteria criteria);

        // Undo transaction buat yang abort recovery
        bool UndoTransaction(int transactionId);
    }
}
