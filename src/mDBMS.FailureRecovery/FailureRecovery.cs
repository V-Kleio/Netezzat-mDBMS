using mDBMS.Common.Classes;
using mDBMS.Common.Interfaces;
using System;


namespace mDBMS.FailureRecovery
{

    public class FailureRecoveryManager : IFailureRecovery, IBufferManager
    {
        private readonly string _logFilePath = "mDBMS.log";
        private byte[] _buffer;

		public FailureRecoveryManager()
		{
			_buffer = new byte[8192]; // 8KB gatau ini hardcod brap
		}
		
        public void WriteLog(ExecutionResult info){}

        public void Recover(RecoverCriteria criteria){}

        public void SaveCheckpoint(){}

        public void WriteToBuffer(byte[] data){}

        public byte[] ReadFromBuffer(int blockId){
            return null;
        }
    }
}