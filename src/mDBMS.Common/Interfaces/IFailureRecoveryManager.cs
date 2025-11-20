using mDBMS.Common.Transaction;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.Common.Interfaces

{
    public interface IFailureRecoveryManager
	{
		//ExecutionResult blom ada classnya, bukan kita yang implement bikin dummy aja klao mo test , tanya klompok CCM
		void WriteLog(ExecutionResult info);

		void SaveCheckpoint();

		//RecoverCriteria udah ada di mDBMS.Common/DTOs
        void Recover(RecoverCriteria criteria);

    }
}