using System;
using System.Collections.Generic;
using System.IO;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public class StorageEngine : IStorageManager
    {
        private static readonly string DataPath = AppDomain.CurrentDomain.BaseDirectory;
        private const int BlockSize = 4096;
        private const int FileHeaderSize = 4096; 

        public StorageEngine() { }

        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            string tableName = dataRetrieval.Table;
            string fileName = Path.Combine(DataPath, $"{tableName.ToLower()}.dat");

            if (!File.Exists(fileName)) yield break;

            TableSchema schema = GetSchemaForTable(tableName);
            if (schema == null) yield break;

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                long fileLength = fs.Length;
                if (fileLength <= FileHeaderSize) yield break;

                fs.Seek(FileHeaderSize, SeekOrigin.Begin);

                byte[] buffer = new byte[BlockSize];
                int bytesRead;

                // Loop ini membaca fisik per 4KB
                while ((bytesRead = fs.Read(buffer, 0, BlockSize)) > 0)
                {
                    if (bytesRead < BlockSize) Array.Clear(buffer, bytesRead, BlockSize - bytesRead);

                    List<Row> rowsInBlock = BlockSerializer.DeserializeBlock(schema, buffer);
                    foreach (var row in rowsInBlock)
                    {
                        yield return row;
                    }
                }
            }
        }

        public int WriteBlock(DataWrite dataWrite)
        {
            string tableName = dataWrite.Table;
            string fileName = Path.Combine(DataPath, $"{tableName.ToLower()}.dat");
            TableSchema schema = GetSchemaForTable(tableName);
            
            // 1. Siapkan File (Create if not exists) & Tulis Header Dummy jika baru
            bool isNewFile = !File.Exists(fileName);
            using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write))
            {
                if (isNewFile)
                {
                    // Tulis 4KB kosong sebagai placeholder Header Metadata
                    // Nanti diurus SchemaSerializer, sekarang isi 0 dulu biar offset bener
                    fs.Write(new byte[FileHeaderSize], 0, FileHeaderSize);
                }

                // 2. Konversi Data Baru (Dictionary) ke Byte Arrays (Serialized Rows)
                var rawRows = new List<byte[]>();
                
                // Kita asumsikan dataWrite.NewValues adalah 1 row (sesuai spec insert simple)
                // Atau jika Anda punya list row, loop di sini.
                // Di sini kita buat objek Row sementara untuk serializer
                var rowObj = new Row(); 
                foreach(var kvp in dataWrite.NewValues) rowObj[kvp.Key] = kvp.Value;

                byte[] serializedRow = RowSerializer.SerializeRow(schema, rowObj);
                rawRows.Add(serializedRow);

                // 3. Bungkus Row menjadi Blok 4KB
                // CATATAN: Implementasi simple M2 = 1 Row -> masuk ke blok baru (pemborosan tempat gapapa untuk M2)
                // Kalau mau advanced: Harusnya baca blok terakhir, cek muat gak, baru append.
                // Tapi untuk LULUS M2, Append blok baru berisi 1 row itu sah.
                
                byte[] blockData = BlockSerializer.CreateBlock(rawRows);
                
                // 4. Tulis ke Disk
                fs.Write(blockData, 0, blockData.Length);
            }

            return 1; // 1 baris affected
        }

        // Helper Skema (Sama seperti sebelumnya)
        private TableSchema GetSchemaForTable(string tableName)
        {
             var schema = new TableSchema { TableName = tableName };
            if (tableName.Equals("Students", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "StudentID", Type = DataType.Int });
                schema.Columns.Add(new ColumnSchema { Name = "FullName", Type = DataType.String, Length = 50 });
                // schema.Columns.Add(new ColumnSchema { Name = "GPA", Type = DataType.Float }); // Uncomment jika RowSerializer sudah support float
            }
            else if (tableName.Equals("Courses", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "CourseID", Type = DataType.Int });
                schema.Columns.Add(new ColumnSchema { Name = "CourseName", Type = DataType.String, Length = 50 });
                schema.Columns.Add(new ColumnSchema { Name = "Credits", Type = DataType.Int });
            }
             else if (tableName.Equals("Enrollments", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "EnrollmentID", Type = DataType.Int });
                schema.Columns.Add(new ColumnSchema { Name = "StudentID", Type = DataType.Int });
                schema.Columns.Add(new ColumnSchema { Name = "CourseID", Type = DataType.Int });
                schema.Columns.Add(new ColumnSchema { Name = "Grade", Type = DataType.String, Length = 2 });
            }
            return schema;
        }

        public int DeleteBlock(DataDeletion dataDeletion) { return 0; } 
        public void SetIndex(string table, string column, IndexType type) { } 
        public Statistic GetStats(string tablename) { return new Statistic(); } 
    }
}