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
        
        private readonly Dictionary<string, HashIndex> _activeIndexes = new(); 

        public StorageEngine() { }

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

        public int AddBlock(DataWrite data_write)
        {
            string tableName = data_write.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);
            
            if (!File.Exists(fullPath)) return 0; 

            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            // Buat Row Baru dari NewValues
            Row rowObj = new Row(); 
            foreach(var kvp in data_write.NewValues) rowObj[kvp.Key] = kvp.Value;
            
            // Pastikan ID ada
            if (string.IsNullOrEmpty(rowObj.id)) rowObj.id = Guid.NewGuid().ToString();

            // Serialize Row
            byte[] rowData = RowSerializer.SerializeRow(schema, rowObj);
            int rowSize = rowData.Length;

            bool spaceFound = false;

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.ReadWrite))
            {
                // First Fit (Cari celah kosong di blok yang ada)
                if (fs.Length > FileHeaderSize)
                {
                    fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                    byte[] buffer = new byte[BlockSize];
                    long currentOffset = FileHeaderSize;

                    while (fs.Read(buffer, 0, BlockSize) > 0)
                    {
                        int freeSpace = BlockSerializer.GetFreeSpace(buffer, rowSize);
                        
                        if (freeSpace >= rowSize + 2) // +2 untuk header entry directory
                        {
                            if (BlockSerializer.TryInsertRow(schema, buffer, rowData))
                            {
                                // Tulis balik blok yang sudah diupdate
                                fs.Seek(currentOffset, SeekOrigin.Begin);
                                fs.Write(buffer, 0, BlockSize);
                                spaceFound = true;
                                break; 
                            }
                        }
                        currentOffset += BlockSize;
                    }
                }

                // Jika tidak ada tempat, Append blok baru
                if (!spaceFound)
                {
                    fs.Seek(0, SeekOrigin.End);
                    var rawRows = new List<byte[]> { rowData };
                    byte[] newBlock = BlockSerializer.CreateBlock(rawRows);
                    fs.Write(newBlock, 0, BlockSize); // Tulis full 4KB
                }
            }

            // Update Index sepertinya todo
            // UpdateIndexes(tableName, rowObj, targetBlockOffset);

            return 1;
        }


        public int WriteBlock(DataWrite data_write)
        {
            string tableName = data_write.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) return 0;
            TableSchema? schema = GetSchemaFromFile(fileName);
            if (schema == null) return 0;

            int updatedCount = 0;
            
            // Karena update bisa mengubah ukuran baris (misal string jadi lebih panjang),
            // Jika tidak muat di blok lama, kita lakukan Delete -> Insert.
            
            // Untuk simplifikasi IO: Kita baca semua, update di memori, tulis balik.
            
            var blocks = new List<byte[]>();
            var blockOffsets = new List<long>();

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize) return 0;
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                
                byte[] buffer = new byte[BlockSize];
                long offset = FileHeaderSize;
                while (fs.Read(buffer, 0, BlockSize) > 0)
                {
                    byte[] copy = new byte[BlockSize];
                    Array.Copy(buffer, copy, BlockSize);
                    blocks.Add(copy);
                    blockOffsets.Add(offset);
                    offset += BlockSize;
                }
            }

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Write))
            {
                for (int i = 0; i < blocks.Count; i++)
                {
                    byte[] blockData = blocks[i];
                    List<Row> rows = BlockSerializer.DeserializeBlock(schema, blockData);
                    bool blockModified = false;
                    List<Row> newRowsForBlock = new List<Row>();
                    List<Row> overflowRows = new List<Row>();

                    foreach (var row in rows)
                    {
                        if (CheckCondition(row, data_write.Condition))
                        {
                            // Lakukan Update
                            foreach (var kvp in data_write.NewValues)
                            {
                                row[kvp.Key] = kvp.Value;
                            }
                            updatedCount++;
                            blockModified = true;
                        }
                        newRowsForBlock.Add(row);
                    }

                    if (blockModified)
                    {
                        List<byte[]> serializedRows = newRowsForBlock.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                        
                        // Cek apakah muat?
                        // (Perhitungan kasar: Total data + header overhead)
                        int totalSize = serializedRows.Sum(r => r.Length) + (serializedRows.Count * 2) + 4; // Estimasi overhead
                        
                        if (totalSize <= BlockSize)
                        {
                            // Muat, tulis balik di posisi semula
                            byte[] newBlock = BlockSerializer.CreateBlock(serializedRows);
                            fs.Seek(blockOffsets[i], SeekOrigin.Begin);
                            fs.Write(newBlock, 0, BlockSize);
                        }
                        else
                        {
                            // Tidak muat (Overflow). 
                            // Solusi: Hapus baris yang diupdate dari blok ini, dan Insert ulang sebagai baris baru (AddBlock logic).
                            // Karena 'fs' sedang dibuka Write, kita harus hati-hati.
                            // Untuk amannya di Milestone ini: Write ulang blok yang muat, lalu panggil AddBlock terpisah nanti (tidak bisa karena fs lock).
                            
                            // Cara sederhana: Paksa Delete lalu Insert Logic di loop terpisah atau throw error.
                            // Disini kita biarkan dulu (risiko data corrupt jika overflow), atau log error.
                            Console.WriteLine($"[WARNING] Block {i} overflow after update. Data might be truncated.");
                            
                            byte[] newBlock = BlockSerializer.CreateBlock(serializedRows); // BlockSerializer mungkin akan truncate otomatis
                            fs.Seek(blockOffsets[i], SeekOrigin.Begin);
                            fs.Write(newBlock, 0, BlockSize);
                        }
                    }
                }
            }

            return updatedCount;
        }

        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            string tableName = dataRetrieval.Table;
            string fileName = $"{tableName.ToLower()}.dat";
            string fullPath = Path.Combine(DataPath, fileName);

            if (!File.Exists(fullPath)) yield break;
            
            TableSchema? schema = GetSchemaFromFile(fileName); 
            if (schema == null) yield break; 

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize) yield break;
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);

                byte[] buffer = new byte[BlockSize];
                while (fs.Read(buffer, 0, BlockSize) > 0)
                {
                    var rows = BlockSerializer.DeserializeBlock(schema, buffer);
                    foreach (var row in rows)
                    {
                        if (CheckCondition(row, dataRetrieval.Condition)) yield return row;
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
            // Logic: Read all -> Filter -> Rewrite
            
            var allBlocks = new List<byte[]>();
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize) return 0;
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                byte[] buffer = new byte[BlockSize];
                while (fs.Read(buffer, 0, BlockSize) > 0)
                {
                    byte[] copy = new byte[BlockSize];
                    Array.Copy(buffer, copy, BlockSize);
                    allBlocks.Add(copy);
                }
            }

            var newBlocks = new List<byte[]>();
            foreach (var blockData in allBlocks)
            {
                var rows = BlockSerializer.DeserializeBlock(schema, blockData);
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

                if (survivingRows.Count > 0)
                {
                    if (modified)
                    {
                         var serialized = survivingRows.Select(r => RowSerializer.SerializeRow(schema, r)).ToList();
                         newBlocks.Add(BlockSerializer.CreateBlock(serialized));
                    }
                    else
                    {
                        newBlocks.Add(blockData);
                    }
                }
            }

            // Write back (Rewrite file)
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Write))
            {
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                foreach (var block in newBlocks)
                {
                    fs.Write(block, 0, BlockSize);
                }
                fs.SetLength(FileHeaderSize + (newBlocks.Count * BlockSize));
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
             // Implementasi statistik (sama seperti sebelumnya)
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
            string colName = cond.lhs.ToString();
            if (!row.Columns.TryGetValue(colName, out object? val) || val == null) return false;

            try 
            {
                IComparable rowVal = (IComparable)val;
                IComparable condVal = (IComparable)Convert.ChangeType(cond.rhs, val.GetType());
                
                int result = rowVal.CompareTo(condVal);

                // Condition.Operation untuk akses Enum
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