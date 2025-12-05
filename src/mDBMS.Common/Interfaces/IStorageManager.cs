using mDBMS.Common.Data;

namespace mDBMS.Common.Interfaces
{
    public interface IStorageManager
    {
        // FailureRecoveryManager.ReadFromBuffer(string tableName,int blockId)
        IEnumerable<Row> ReadBlock(DataRetrieval data_retrieval); // KE BUFFER FALLBACK KE DISK

        // FailureRecoveryManager.WriteToBuffer(Page page)
        int WriteBlock(DataWrite data_write); // KE BUFFER
        int AddBlock(DataWrite data_write); // KE BUFFER 
        int DeleteBlock(DataDeletion data_deletion); // KE BUFFER

        /// FRM MANGGGIL BUAT FLUSH(INSERT UPDATE DELETE)
        int WriteDisk(Page page); // KE DISK
        
        /// <summary>
        /// Mengatur atau membuat indeks pada kolom tertentu di tabel
        /// </summary>
        /// <param name="table">Nama tabel tempat untuk indeks yang akan dibuat</param>
        /// <param name="column">Nama kolom yang akan diindeks</param>
        /// <param name="type">Tipe indeks yang akan dibuat</param>
        void SetIndex(string table, string column, IndexType type);

        /// <summary>
        /// Mendapatkan informasi statistik dari sistem penyimpanan
        /// </summary>
        /// <param name="tableName">Nama tabel yang ingin diambil statistiknya</param>
        /// <returns>Objek yang berisi informasi statistik</returns>
        Statistic GetStats(string tableName);
    }
}