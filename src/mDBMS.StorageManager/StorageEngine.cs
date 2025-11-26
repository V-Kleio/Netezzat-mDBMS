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
            string tableName = dataRetrieval.Table;
            string fileName = $"{tableName.ToLower()}.dat";
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
            string tableName = dataWrite.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);
            
            if (!File.Exists(fullPath)) return 0; 

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            // Buat object Row baru
            Row rowObj = new Row(); 
            foreach(var kvp in dataWrite.NewValues) rowObj[kvp.Key] = kvp.Value;
            
            // Serialize Row menjadi byte array
            byte[] rowData = RowSerializer.SerializeRow(schema, rowObj);
            
            // TODO (Milestone 3): Logic untuk mencari Free Space di blok yang ada (Bitmap/FreeList).
            // Untuk Milestone 2: Append Only dulu (Selalu buat blok baru/tambah di akhir).
            
            long newBlockOffset;
            using (var fs = new FileStream(fullPath, FileMode.Append, FileAccess.Write))
            {
                newBlockOffset = fs.Position; 
                // Buat blok baru hanya dengan 1 baris
                var rawRows = new List<byte[]> { rowData };
                byte[] blockData = BlockSerializer.CreateBlock(rawRows);
                fs.Write(blockData, 0, blockData.Length);
            }

            // Update Index jika ada
            UpdateIndexes(tableName, rowObj, newBlockOffset);

            return 1; // Return 1 baris affected
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
            var stats = new Statistic { Table = tablename };

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
            long fileSize = new FileInfo(fullPath).Length;

            // Baca semua blok, filter row yang match kondisi, rebuild blok tanpa row tersebut
            var allBlocks = new List<byte[]>();
            var blockOffsets = new List<long>();

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize) return 0;

                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                byte[] buffer = new byte[BlockSize];
                long currentOffset = FileHeaderSize;

                // Baca semua blok
                while (fs.Read(buffer, 0, BlockSize) > 0)
                {
                    byte[] blockCopy = new byte[BlockSize];
                    Buffer.BlockCopy(buffer, 0, blockCopy, 0, BlockSize);
                    allBlocks.Add(blockCopy);
                    blockOffsets.Add(currentOffset);
                    currentOffset += BlockSize;
                }
            }

            // Process setiap blok: filter out rows yang match deletion condition
            var newBlocks = new List<byte[]>();

            for (int i = 0; i < allBlocks.Count; i++)
            {
                var blockData = allBlocks[i];
                var rows = BlockSerializer.DeserializeBlock(schema, blockData);

                // Filter row: Keep hanya yang TIDAK match kondisi delete
                var survivingRows = new List<Row>();
                int deletedInThisBlock = 0;

                foreach (var row in rows)
                {
                    if (CheckCondition(row, dataDeletion.Condition))
                    {
                        deletedInThisBlock++;
                        // Hapus dari index jika ada
                        RemoveFromIndexes(tableName, row, blockOffsets[i]);
                    }
                    else
                    {
                        survivingRows.Add(row);
                    }
                }

                deletedCount += deletedInThisBlock;

                // Rebuild blok hanya dengan surviving rows
                if (survivingRows.Count > 0)
                {
                    var serializedRows = new List<byte[]>();
                    foreach (var row in survivingRows)
                    {
                        serializedRows.Add(RowSerializer.SerializeRow(schema, row));
                    }
                    newBlocks.Add(BlockSerializer.CreateBlock(serializedRows));
                }
                // Jika blok kosong (semua row dihapus), jangan masukkan ke newBlocks
            }

            // Tulis ulang file dengan blok baru (tanpa blok yang kosong)
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Write))
            {
                // Preserve file header (4KB pertama)
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);

                foreach (var block in newBlocks)
                {
                    fs.Write(block, 0, block.Length);
                }

                // Truncate file jika ada blok yang dihapus
                fs.SetLength(FileHeaderSize + (newBlocks.Count * BlockSize));
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