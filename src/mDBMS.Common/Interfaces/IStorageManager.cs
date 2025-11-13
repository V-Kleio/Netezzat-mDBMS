using System.Collections.Generic;
using System.Data;
using mDBMS.Common.Models;

namespace mDBMS.Common.Interfaces
{
    public interface IStorageManager
    {
        /// <summary>
        /// Membaca satu atau beberapa baris data dari tabel sesuai kondisi pada data_retrieval
        /// </summary>
        /// <param name="data_retrieval">Objek yang berisi kondisi untuk pembacaan data</param>
        /// <returns>Daftar baris yang dibaca dari tabel sesuai kondisi</returns>
        IEnumerable<Row> ReadBlock(DataRetrieval data_retrieval);

        /// <summary>
        /// Menulis atau memperbarui satu atau beberapa baris data pada tabel sesuai kondisi pada data_write
        /// </summary>
        /// <param name="data_write">Objek yang berisi data yang akan ditulis atau diperbarui dan kondisinya</param>
        /// <returns>Jumlah baris yang berhasil ditulis atau diperbarui</returns>
        int WriteBlock(DataWrite data_write);

        /// <summary>
        /// Menghapus satu atau beberapa baris data dari tabel sesuai kondisi pada data_deletion
        /// </summary>
        /// <param name="data_deletion">Objek yang berisi kondisi untuk penghapusan data</param>
        /// <returns>Jumlah baris yang berhasil dihapus</returns>
        int DeleteBlock(DataDeletion data_deletion);

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