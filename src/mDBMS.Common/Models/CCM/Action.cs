namespace mDBMS.Common.Models.CCM
{
    /// <summary>
    /// DTO yang merepresentasikan sebuah aksi (read/write)
    /// Sesuai spesifikasi halaman 5.
    /// </summary>
    public class Action
    {
        /// <summary>
        /// Tipe aksi yang akan dilakukan, 'write' atau 'read'.
        /// </summary>
        public string action { get; set; } = string.Empty;
    }
}