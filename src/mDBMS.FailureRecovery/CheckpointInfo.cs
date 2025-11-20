namespace mDBMS.FailureRecovery
{
    /// Class untuk mengelola struktur checkpoint
    /// Checkpoint menyimpan informasi tentang transaksi yang masih aktif
    public class CheckpointInfo
    {
        public long LSN { get; set; }
        public DateTime Timestamp { get; set; }
        public List<int> ActiveTransactions { get; set; }
        public long LastCheckpointLSN { get; set; } // LSN dari checkpoint sebelumnya

        public CheckpointInfo()
        {
            ActiveTransactions = new List<int>();
        }

        public string Serialize()
        {
            return $"{LSN}|{Timestamp:yyyy-MM-dd HH:mm:ss.ffffff}|" +
                   $"{string.Join(",", ActiveTransactions)}|{LastCheckpointLSN}";
        }

        public static CheckpointInfo Deserialize(string line)
        {
            var parts = line.Split('|');
            var info = new CheckpointInfo
            {
                LSN = long.Parse(parts[0]),
                Timestamp = DateTime.Parse(parts[1]),
                LastCheckpointLSN = long.Parse(parts[3])
            };

            if (!string.IsNullOrEmpty(parts[2]))
            {
                info.ActiveTransactions = new List<int>(
                    Array.ConvertAll(parts[2].Split(','), int.Parse)
                );
            }

            return info;
        }
    }
}