using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public class StorageEngine : IStorageManager
    {
        private static readonly string DataPath = GetDataPath();
        private const int BlockSize = 4096;
        private const int FileHeaderSize = 4096;

        private readonly Dictionary<string, HashIndex> _activeIndexes = new();
        private readonly IBufferManager? _bufferManager;
        /// <summary>
        /// Gets the shared data path for all mDBMS projects
        /// </summary>
        private static string GetDataPath()
        {
            string currentDir = Directory.GetCurrentDirectory();
            DirectoryInfo? dir = new DirectoryInfo(currentDir);

            while (dir != null)
            {
                if (dir.GetFiles("*.sln").Length > 0)
                {
                    return Path.Combine(dir.FullName, "data");
                }
                dir = dir.Parent;
            }

            string? customPath = Environment.GetEnvironmentVariable("MDBMS_DATA_PATH");
            if (!string.IsNullOrEmpty(customPath))
            {
                return customPath;
            }

            return Path.Combine(currentDir, "data");
        }

        public StorageEngine(IBufferManager? bufferManager = null)
        {
            Console.WriteLine($"DataPath set at: {DataPath}");
            _bufferManager = bufferManager;
        }

        // Write dari buffer ke disk
        public int WriteDisk(Page page)
        {
            string fileName = $"{page.TableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);


            try
            {
                using (var fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    long offset = FileHeaderSize + ((long)page.BlockID * BlockSize);
                    fs.Seek(offset, SeekOrigin.Begin);
                    fs.Write(page.Data, 0, BlockSize);
                }
                return 1; // Sukses
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[StorageEngine] Error writing disk: {ex.Message}");
                return 0; // Gagal
            }
        }

        // Ini INSERT block
        public int AddBlock(DataWrite data_write)
        {
            string tableName = data_write.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath))
            {
                throw new Exception("tabel tidak ditemukan");
            }

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            // Buat Row Baru dari NewValues
            Row rowObj = new Row();
            foreach(var kvp in data_write.NewValues) rowObj[kvp.Key] = kvp.Value;

            // Pastikan ID ada
            if (string.IsNullOrEmpty(rowObj.id)) rowObj.id = Guid.NewGuid().ToString();

            // Serialize Row (buat itung ukuran byte)
            byte[] rowData = RowSerializer.SerializeRow(schema, rowObj);
            int rowSize = rowData.Length;

            int targetBlockID = -1;
            byte[]? targetBuffer = null;
            bool spaceFound = false;

            // Tentukan Loop Limit (Berdasarkan ukuran file di Disk)
            long fileLength = new FileInfo(fullPath).Length;
            int totalBlocks = (int)((fileLength - FileHeaderSize) / BlockSize);

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                for (int i = 0; i < totalBlocks; i++)
                {
                    byte[] currentBlockData = new byte[BlockSize];
                    bool isDataFromBuffer = false;

                    // Buffer check
                    if (_bufferManager != null)
                    {
                        byte[] bufferedData = _bufferManager.ReadFromBuffer(tableName, i);
                        if (bufferedData != null && bufferedData.Length == BlockSize)
                        {
                            currentBlockData = bufferedData;
                            isDataFromBuffer = true;
                        }
                    }

                    // Gada ada di Buffer, baru baca dari Disk
                    if (!isDataFromBuffer)
                    {
                        fs.Seek(FileHeaderSize + (i * BlockSize), SeekOrigin.Begin);
                        fs.Read(currentBlockData, 0, BlockSize);
                    }

                    // Inserting row, check free space di block tsbt
                    int freeSpace = BlockSerializer.GetFreeSpace(currentBlockData, rowSize);

                    if (freeSpace >= 0)
                    {
                        if (BlockSerializer.TryInsertRow(schema, currentBlockData, rowData))
                        {
                            targetBlockID = i;
                            targetBuffer = currentBlockData; // Ini sekarang berisi data lama + row baru
                            spaceFound = true;
                            break;
                        }
                    }
                }
            }

            // Jika tidak ada tempat, Append blok baru
            if (!spaceFound)
            {
                // Block ID baru adalah index terakhir (totalBlocks)
                targetBlockID = totalBlocks;

                var rawRows = new List<byte[]> { rowData };
                targetBuffer = BlockSerializer.CreateBlock(rawRows);
            }

            // Write to buffer (requires buffer manager)
            if (targetBuffer != null && targetBlockID >= 0)
            {
                if (_bufferManager == null)
                {
                    Console.Error.WriteLine("[StorageEngine] Buffer manager not initialized!");
                    return 0;
                }

                // Write to buffer
                Page page = new(tableName, targetBlockID, targetBuffer, true);
                _bufferManager.WriteToBuffer(page);
                return 1; // success
            }
            return 0; // fail
        }

        // Ini UPDATE block
        public int WriteBlock(DataWrite data_write)
        {
            string tableName = data_write.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath))
            {
                throw new Exception("tabel tidak ditemukan");
            }

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            int updatedCount = 0;

            // Karena update bisa mengubah ukuran baris (misal string jadi lebih panjang),
            // Jika tidak muat di blok lama, kita lakukan Delete -> Insert.

            List<Row> rowsToMigrate = new List<Row>();

            // Untuk simplifikasi IO: Kita baca semua, update di memori, tulis balik.

            // Tentukan Loop Limit (Berdasarkan ukuran file di Disk)
            long fileLength = new FileInfo(fullPath).Length;
            int totalBlocks = (int)((fileLength - FileHeaderSize) / BlockSize);

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                for (int i = 0; i < totalBlocks; i++)
                {
                    byte[] currentBlockData = new byte[BlockSize];
                    bool isDataFromBuffer = false;
                    if (_bufferManager != null)
                    {
                        byte[] bufferedData = _bufferManager.ReadFromBuffer(tableName, i);
                        if (bufferedData != null && bufferedData.Length == BlockSize)
                        {
                            currentBlockData = bufferedData;
                            isDataFromBuffer = true;
                        }
                    }
                    // alau gak ada di buffer, baca Disk
                    if (!isDataFromBuffer)
                    {
                        fs.Seek(FileHeaderSize + (i * BlockSize), SeekOrigin.Begin);
                        fs.Read(currentBlockData, 0, BlockSize);
                    }

                    List<Row> rows = BlockSerializer.DeserializeBlock(schema, currentBlockData);
                    bool blockModified = false;

                    foreach (var row in rows)
                    {
                        if (CheckCondition(row, data_write.Condition))
                        {
                            foreach (var kvp in data_write.NewValues)
                            {
                                row[kvp.Key] = kvp.Value;
                            }
                            updatedCount++;
                            blockModified = true;
                        }
                    }

                    if (blockModified)
                    {
                        // Cek Ukuran Baru
                        List<byte[]> serializedRows = rows.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                        int totalSize = serializedRows.Sum(r => r.Length) + (serializedRows.Count * 2) + 4;

                        // LOGIC PENANGANAN OVERFLOW (Delete -> Insert)
                        while (totalSize > BlockSize && rows.Count > 0)
                        {
                            // Ambil row paling belakang
                            Row victim = rows[rows.Count - 1];

                            // Hapus dari blok ini (DELETE)
                            rows.RemoveAt(rows.Count - 1);

                            // Masukkan ke antrian untuk dipindah (INSERT nanti)
                            rowsToMigrate.Add(victim);

                            // Hitung ulang size
                            serializedRows = rows.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                            totalSize = serializedRows.Sum(r => r.Length) + (serializedRows.Count * 2) + 4;

                            Console.WriteLine($"[StorageEngine] Block {i} overflow. Migrating Row {victim.id}...");
                        }
                        // Simpan blok yang sudah aman (muat)
                        byte[] newBlock = BlockSerializer.CreateBlock(serializedRows);
                        if (_bufferManager != null)
                        {
                            Page page = new(tableName, i, newBlock, true);
                            _bufferManager.WriteToBuffer(page);
                        }
                    }
                }
            }

            foreach(var row in rowsToMigrate)
            {
                var insertData = new DataWrite(tableName, row.Columns, null);
                AddBlock(insertData);
            }
            return updatedCount;
        }

        // Read block
        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            string tableName = dataRetrieval.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath))
            {
                throw new Exception("tabel tidak ditemukan");
            }

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) yield break;

            // HITUNG TOTAL BLOK DI DISK SEKARANG
            long fileLength = new FileInfo(fullPath).Length;
            int totalBlocks = (int)((fileLength - FileHeaderSize) / BlockSize);

            using var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read);
            if (fileLength <= FileHeaderSize) yield break;

            if (_bufferManager == null)
            {
                Console.Error.WriteLine("[StorageEngine] Buffer manager not initialized!");
                yield break;
            }

            for (int blockID = 0; blockID < totalBlocks; blockID++)
            {
                byte[] currentBlockData = new byte[BlockSize];
                bool isDataFromBuffer = false;

                // Try to read from buffer first
                byte[] bufferData = _bufferManager.ReadFromBuffer(tableName, blockID);
                if (bufferData != null && bufferData.Length == BlockSize)
                {
                    currentBlockData = bufferData;
                    isDataFromBuffer = true;
                }

                // Gada di buffer, read from disk
                if (!isDataFromBuffer)
                {
                    fs.Seek(FileHeaderSize + ((long)blockID * BlockSize), SeekOrigin.Begin);

                    int bytesRead = fs.Read(currentBlockData, 0, BlockSize);

                    if (bytesRead != BlockSize)
                    {
                        Console.WriteLine($"[WARNING] Block {blockID} could only read {bytesRead} bytes.");
                        continue;
                    }
                }

                // Deserialize + filter
                var rows = BlockSerializer.DeserializeBlock(schema, currentBlockData);
                foreach (var row in rows)
                {
                    if (CheckCondition(row, dataRetrieval.Condition)) yield return row;
                }
            }
        }

        // Delete block
        public int DeleteBlock(DataDeletion dataDeletion)
        {
            string tableName = dataDeletion.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath))
            {
                throw new Exception("tabel tidak ditemukan");
            }

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            int deletedCount = 0;
            // Read all -> Filter -> Rewrite

            long fileLength = new FileInfo(fullPath).Length;
            int totalBlocks = (int)((fileLength - FileHeaderSize) / BlockSize);

            if (_bufferManager == null)
            {
                Console.Error.WriteLine("[StorageEngine] Buffer manager not initialized!");
                return 0;
            }

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                for (int blockID = 0; blockID < totalBlocks; blockID++)
                {
                    byte[] currentBlockData = new byte[BlockSize];
                    bool isDataFromBuffer = false;

                    // A. CEK BUFFER DULU (Wajib!)
                    byte[]? bufferedData = _bufferManager.ReadFromBuffer(tableName, blockID);
                    if (bufferedData != null && bufferedData.Length == BlockSize)
                    {
                        currentBlockData = bufferedData;
                        isDataFromBuffer = true;
                    }

                    // B. Kalau gak ada di buffer, baca Disk
                    if (!isDataFromBuffer)
                    {
                        fs.Seek(FileHeaderSize + ((long)blockID * BlockSize), SeekOrigin.Begin);
                        fs.Read(currentBlockData, 0, BlockSize);
                    }

                    // C. Deserialize & Filter Logic
                    var rows = BlockSerializer.DeserializeBlock(schema, currentBlockData);
                    var survivingRows = new List<Row>();
                    bool modified = false;

                    foreach (var row in rows)
                    {
                        if (CheckCondition(row, dataDeletion.Condition))
                        {
                            deletedCount++;
                            modified = true;
                        }
                        else
                        {
                            survivingRows.Add(row);
                        }
                    }

                    if (modified)
                    {
                        var serialized = survivingRows.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                        byte[] newBlock = BlockSerializer.CreateBlock(serialized);

                        // Write to buffer
                        Page page = new(tableName, blockID, newBlock, true);
                            _bufferManager.WriteToBuffer(page);
                    }

                }

            }
            return deletedCount;
        }

        public void SetIndex(string table, string column, IndexType type)
        {
            if (type != IndexType.Hash) return;
            string indexKey = $"{table}.{column}";
            _activeIndexes[indexKey] = new HashIndex(table, column);
            // Index logic implementation here if needed
        }

        public Statistic GetStats(string tablename)
        {
             // Implementasi statistik
             return new Statistic { Table = tablename };
        }

        // HELPERS

        private TableSchema? GetSchemaFromFile(string fileName)
        {
            try
            {
                string fullPath = Path.Combine(DataPath, fileName);
                if (!File.Exists(fullPath)) return null;
                return SchemaSerializer.ReadSchema(fullPath);
            }
            catch { return null; }
        }

        private bool CheckCondition(Row row, IEnumerable<IEnumerable<Condition>>? conditions)
        {
            if (conditions == null || !conditions.Any()) return true;

            foreach (var andGroup in conditions)
            {
                bool isGroupMatch = true;
                foreach (var cond in andGroup)
                {
                    if (!EvaluateSingleCondition(row, cond))
                    {
                        isGroupMatch = false;
                        break;
                    }
                }
                if (isGroupMatch) return true;
            }
            return false;
        }

        private bool EvaluateSingleCondition(Row row, Condition cond)
        {
            // Handle null pada lhs dan cek apakah string kosong
            string? colName = cond.lhs?.ToString();

            if (string.IsNullOrEmpty(colName) || !row.Columns.TryGetValue(colName, out object? val) || val == null)
                return false;

            try
            {
                // Handle null pada rhs dan gunakan nullable casting
                if (cond.rhs == null) return false;

                IComparable rowVal = (IComparable)val;
                IComparable? condVal = (IComparable?)Convert.ChangeType(cond.rhs, val.GetType());

                if (condVal == null) return false;

                int result = rowVal.CompareTo(condVal);

                switch (cond.opr)
                {
                    case Condition.Operation.EQ: return result == 0;
                    case Condition.Operation.NEQ: return result != 0;
                    case Condition.Operation.LT: return result < 0;
                    case Condition.Operation.LEQ: return result <= 0;
                    case Condition.Operation.GT: return result > 0;
                    case Condition.Operation.GEQ: return result >= 0;
                    default: return false;
                }
            }
            catch { return false; }
        }
    }
}


// INSERT UPDATE DELETE KE BUFFER
// FRM -> FailureRecoveryManager.WriteToBuffer(Page page)
// sm -> invoke FRM

// READ KE BUFFER (fallback write ke disk kalo gada (Array.Empty<byte>()))
// FRM -> FailureRecoveryManager.ReadFromBuffer(string tableName,int blockId)
// Sm -> ReadBlock() {FailureRecoveryManager.ReadFromBuffer(string,int)}

// WRITE / UPDATE / DELETE KE DISK (FIX)
// FRM -> flushBuffer(Page) {SM.WriteDisk(Page)}
// Sm ->  void WriteDisk(Page)

// Sm -> invoke readfrombuffer kalo mau read
