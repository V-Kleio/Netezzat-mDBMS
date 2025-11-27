namespace mDBMS.Common.Interfaces;

using mDBMS.Common.Data;

public interface IBufferManager
{
    void WriteToBuffer(Page page);
    byte[] ReadFromBuffer(string tableName, int blockId);

    /// <summary>
    /// Ambil semua dirty pages yang perlu di-persist ke disk
    /// </summary>
    List<Page> GetDirtyPages();

    /// <summary>
    /// Flush (kosongkan) semua buffer dan return dirty pages
    /// </summary>
    List<Page> FlushAll();
}
