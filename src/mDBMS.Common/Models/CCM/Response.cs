namespace mDBMS.Common.Models.CCM
{
    /// <summary>
    /// DTO yang merepresentasikan respons dari Concurrency Control Manager.
    /// Sesuai spesifikasi halaman 5.
    /// </summary>
    public class Response
    {
        /// <summary>
        /// Menentukan apakah aksi diizinkan (true) atau tidak (false).
        /// </summary>
        public bool allowed { get; set; }

        /// <summary>
        /// ID Transaksi yang terkait dengan respons ini.
        /// </summary>
        public int transaction_id { get; set; }
    }
}