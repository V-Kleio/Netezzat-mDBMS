using mDBMS.Common.Transaction;
using mDBMS.Common.Interfaces;

namespace mDBMS.FailureRecovery
{
    public class FailureRecoveryManager : IFailureRecoveryManager, IBufferManager
    {
        // private readonly string _logFilePath = "mDBMS.log";
        private byte[] _buffer;

		public FailureRecoveryManager()
		{
			_buffer = new byte[8192]; // 8KB gatau ini hardcod brap
		}

        public void WriteLog(ExecutionResult info)
        {
            // stub buat fase 1
            Console.WriteLine($"[STUB FRM]: WriteLog dipanggil untuk kueri '{info.Query}'");
        }

        public void Recover(RecoverCriteria criteria)
        {
            Console.WriteLine($"[STUB FRM]: Recover dipanggil untuk TransactionId '{criteria.TransactionId}' pada Timestamp '{criteria.Timestamp}'");
        }

        public void SaveCheckpoint()
        {
            Console.WriteLine("[STUB FRM]: SaveCheckpoint dipanggil");
        }

        public void WriteToBuffer(byte[] data)
        {
            Console.WriteLine($"[STUB FRM]: WriteToBuffer dipanggil, len={data?.Length ?? 0}");
        }

        public byte[] ReadFromBuffer(int blockId){
            Console.WriteLine($"[STUB FRM-BUFFER]: ReadFromBuffer dipanggil, blockId={blockId}");
            return new byte[0]; // dummy return
        }
    }
}
