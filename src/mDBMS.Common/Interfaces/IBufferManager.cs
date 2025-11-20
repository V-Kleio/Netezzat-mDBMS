namespace mDBMS.Common.Interfaces;
using mDBMS.Common.Data;
{
	public interface IBufferManager
	{
		void WriteToBuffer(Page page);
		byte[] ReadFromBuffer(string tableName, int blockId);
	}
}