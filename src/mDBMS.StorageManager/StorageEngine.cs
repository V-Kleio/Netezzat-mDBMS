using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public class StorageEngine : IStorageManager
    {
        private static readonly string DataPath = AppDomain.CurrentDomain.BaseDirectory;
        private const int BlockSize = 4096;
        private const int FileHeaderSize = 4096;

        // Memory-based Index Storage
        private readonly Dictionary<string, HashIndex> _activeIndexes = new();

        public StorageEngine() { }

        // Helper untuk membaca Schema dari header file
        private TableSchema? GetSchemaFromFile(string fileName)
        {
            try
            {
                string fullPath = Path.Combine(DataPath, fileName);
                if (!File.Exists(fullPath)) return null;
                return SchemaSerializer.ReadSchema(fullPath);
            }
            catch
            {
                return null;
            }
        }

        // ReadBlock
        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            string fileName = $"{page.TableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) yield break;

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) yield break;

            // Cek apakah query bisa menggunakan Index
            // Syarat: Ada kondisi WHERE x = y, dan kolom x memiliki Index
            List<long>? targetBlockOffsets = null;
            bool useIndex = false;
            Condition? cond = dataRetrieval.Condition;

            if (cond != null && cond.opr == Condition.Operation.EQ)
            {
                string indexKey = $"{tableName}.{cond.lhs}";
                if (_activeIndexes.TryGetValue(indexKey, out HashIndex? index))
                {
                    // Konversi value string (rhs) ke tipe data asli kolom untuk pencarian Hash
                    object? searchKey = ConvertToColumnType(schema, cond.lhs, cond.rhs);

                    if (searchKey != null)
                    {
                        targetBlockOffsets = index.GetBlockOffsets(searchKey);
                        // Jika offset ditemukan, aktifkan mode Index Scan
                        if (targetBlockOffsets != null && targetBlockOffsets.Count > 0)
                        {
                            useIndex = true;
                            // [DEBUG] Uncomment line below to prove Index Usage
                            // Console.WriteLine($"[SM OPTIMIZATION] Using Hash Index on {cond.lhs}");
                        }
                    }
                }
            }

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                // INDEX SCAN (Lompat ke Blok Spesifik)
                if (useIndex && targetBlockOffsets != null)
                {
                    byte[] buffer = new byte[BlockSize];
                    foreach (long offset in targetBlockOffsets)
                    {
                        fs.Seek(offset, SeekOrigin.Begin);
                        if (fs.Read(buffer, 0, BlockSize) > 0)
                        {
                            var rows = BlockSerializer.DeserializeBlock(schema, buffer);
                            foreach (var row in rows)
                            {
                                // Filter ulang di memori karena 1 blok bisa berisi banyak baris (Hash Collision di level blok)
                                if (CheckCondition(row, cond)) yield return row;
                            }
                        }
                    }
                }
                // LINEAR SCAN (Baca Semua Blok)
                else
                {
                    // Lewati File Header (4KB pertama)
                    if (fs.Length <= FileHeaderSize) yield break;
                    fs.Seek(FileHeaderSize, SeekOrigin.Begin);

                    byte[] buffer = new byte[BlockSize];
                    while (fs.Read(buffer, 0, BlockSize) > 0)
                    {
                        var rows = BlockSerializer.DeserializeBlock(schema, buffer);
                        foreach (var row in rows)
                        {
                            // Jika ada kondisi (WHERE), filter. Jika tidak, kembalikan semua.
                            if (CheckCondition(row, cond)) yield return row;
                        }
                    }
                }
            }
        }

        public int WriteBlock(DataWrite dataWrite)
        {
            string tableName = data_write.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) return 0;

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            // 1. Siapkan Data Row Baru
            Row rowObj = new Row();
            foreach(var kvp in dataWrite.NewValues) rowObj[kvp.Key] = kvp.Value;
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
                // First Fit (Cari celah kosong di blok yang ada)
                for (int i = 0; i < totalBlocks; i++)
                {
                    byte[] currentBlockData = new byte[BlockSize];
                    bool isDataFromBuffer = false;

                    // Buffer check
                    if (_bufferManager != null)
                    {
                        // Cek apakah muat? (Butuh RowSize + 2 byte pointer)
                        int freeSpace = BlockSerializer.GetFreeSpace(buffer, rowSize);

                        if (freeSpace >= rowSize + 2)
                        {
                            // Sisipkan di sini jika muat
                            if (BlockSerializer.TryInsertRow(schema, buffer, rowData))
                            {
                                // Mundur ke awal blok ini untuk menimpa
                                fs.Seek(currentOffset, SeekOrigin.Begin);
                                fs.Write(buffer, 0, BlockSize);

                                targetBlockOffset = currentOffset;
                                spaceFound = true;
                                break;
                            }
                        }
                    }
                }
            }

                // 3. Jika tidak ada yang muat -> Append baru
                if (!spaceFound)
                {
                    // Pindah ke paling ujung file
                    fs.Seek(0, SeekOrigin.End);
                    targetBlockOffset = fs.Position;

                    // Buat blok baru
                    var rawRows = new List<byte[]> { rowData };
                    byte[] newBlock = BlockSerializer.CreateBlock(rawRows);
                    fs.Write(newBlock, 0, newBlock.Length);
                }
            }

            // 4. Update Index karena offset dinamis
            UpdateIndexes(tableName, rowObj, targetBlockOffset);

            return 1;
        }

        // SetIndex
        public void SetIndex(string table, string column, IndexType type)
        {
            if (type != IndexType.Hash)
            {
                Console.WriteLine("[SM] Hanya Hash Index yang didukung saat ini.");
                return;
            }

            string indexKey = $"{table}.{column}";
            var index = new HashIndex(table, column);

            string fileName = $"{table.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) return 0;
            TableSchema? schema = GetSchemaFromFile(fileName);

            if (schema != null && File.Exists(fullPath))
            {
                using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
                {
                    if (fs.Length > FileHeaderSize)
                    {
                        // 1. Scan Seluruh File (Linear Scan Internal)
                        fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                        long currentOffset = FileHeaderSize;
                        byte[] buffer = new byte[BlockSize];

                        while (fs.Read(buffer, 0, BlockSize) > 0)
                        {
                            // 2. Deserialize Blok
                            List<Row> rows = BlockSerializer.DeserializeBlock(schema, buffer);

                            // 3. Ambil Nilai Kolom & Masukkan ke Index
                            foreach (var row in rows)
                            {
                                if (row.Columns.TryGetValue(column, out object? val) && val != null)
                                {
                                    // Mapping: Value -> Block Offset
                                    index.Add(val, currentOffset);
                                }
                            }
                            currentOffset += BlockSize;
                        }
                    }
                }
            }

            _activeIndexes[indexKey] = index;
            Console.WriteLine($"[SM] Index '{column}' pada tabel '{table}' berhasil dibangun.");
        }

        // GetStats
        public Statistic GetStats(string tablename)
        {
            string fileName = $"{tablename.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) return stats;
            var schema = GetSchemaFromFile(fileName);
            if (schema == null) return stats;

            long fileSize = new FileInfo(fullPath).Length;
            long dataSize = fileSize - FileHeaderSize;

            // l_r (Ukuran Tuple rata-rata/tetap)
            stats.TupleSize = BlockSerializer.CalculateRowSize(schema);

            // f_r (Blocking Factor: Berapa row muat di 1 blok)
            // Rumus: (BlockSize - HeaderBlok) / (TupleSize + UkuranEntryDirectory)
            stats.BlockingFactor = (BlockSize - 4) / (stats.TupleSize + 2);

            // b_r (Jumlah Blok)
            stats.BlockCount = (int)(dataSize / BlockSize);
            if (dataSize > 0 && dataSize % BlockSize != 0) stats.BlockCount++;

            // n_r (Jumlah Tuple Total)
            stats.TupleCount = CountTotalRows(fullPath);

            // V(A,r) Distinct Values (Asumsi Worst Case = TupleCount untuk simplifikasi)
            stats.DistinctValues = stats.TupleCount;

            return stats;
        }

        // Helper: Hitung total row tanpa deserialize full (Cukup baca 2 byte header blok)
        private int CountTotalRows(string path)
        {
            int total = 0;
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize) return 0;
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                byte[] buf = new byte[2];

                while (fs.Position < fs.Length)
                {
                    int read = fs.Read(buf, 0, 2);
                    if (read < 2) break;

                    total += BitConverter.ToUInt16(buf, 0);

                    // Lompat ke blok berikutnya (BlockSize - 2 byte yang baru dibaca)
                    long skip = BlockSize - 2;
                    if (fs.Position + skip <= fs.Length) fs.Seek(skip, SeekOrigin.Current);
                    else break;
                }
            }
            return total;
        }

        // Helper Logic
        private object? ConvertToColumnType(TableSchema schema, string colName, string valString)
        {
            var colDef = schema.Columns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
            if (colDef == null) return null;

            try
            {
                if (colDef.Type == DataType.Int) return int.TryParse(valString, out int i) ? i : null;
                if (colDef.Type == DataType.Float) return float.TryParse(valString, out float f) ? f : null;
                return valString;
            }
            catch { return null; }
        }

        private bool CheckCondition(Row row, Condition? cond)
        {
            if (cond == null) return true;
            if (row.Columns.TryGetValue(cond.lhs, out object? val) && val != null)
            {
                // Bandingkan sebagai string agar aman
                return val.ToString() == cond.rhs;
            }
            return false;
        }

        private void UpdateIndexes(string table, Row row, long blockOffset)
        {
            foreach(var index in _activeIndexes.Values)
            {
                if (index.TableName.Equals(table, StringComparison.OrdinalIgnoreCase))
                {
                    if (row.Columns.TryGetValue(index.ColumnName, out object? val) && val != null)
                    {
                        index.Add(val, blockOffset);
                    }
                }
            }
        }

        public int DeleteBlock(DataDeletion dataDeletion)
        {
            string tableName = dataDeletion.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) return 0;

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
                        if (survivingRows.Count > 0)
                        {
                            var serialized = survivingRows.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                            byte[] newBlock = BlockSerializer.CreateBlock(serialized);

                            // Write to buffer
                            Page page = new(tableName, blockID, newBlock, true);
                            _bufferManager.WriteToBuffer(page);
                        }
                        // If no surviving rows, block is empty - could handle deletion
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

            return deletedCount;
        }

        // Helper: Remove entry dari index
        private void RemoveFromIndexes(string table, Row row, long blockOffset)
        {
            foreach (var index in _activeIndexes.Values)
            {
                if (index.TableName.Equals(table, StringComparison.OrdinalIgnoreCase))
                {
                    if (row.Columns.TryGetValue(index.ColumnName, out object? val) && val != null)
                    {
                        index.Remove(val, blockOffset);
                    }
                }
            }
        }
    }
}
