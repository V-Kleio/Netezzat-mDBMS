namespace mDBMS.Common.Interfaces
{
	public interface IBufferManager
	{
		void WriteToBuffer(byte[] data);
		byte[] ReadFromBuffer(int blockId);
	}
}