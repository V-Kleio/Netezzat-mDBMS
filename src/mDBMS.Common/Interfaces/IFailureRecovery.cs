using mDBMS.Common.Classes;
using System;

namespace mDBMS.Common.Interfaces
{
    public interface IFailureRecovery
	{
		//ExecutionResult blom ada classnya, bukan kita yang implement bikin dummy aja klao mo test , tanya klompok CCM
		void WriteLog(ExecutionResult info);
		
		void SaveCheckpoint();

		//RecoverCriteria udah ada di mDBMS.Common/Classes
        void Recover(RecoverCriteria criteria);
    }
}