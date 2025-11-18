using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using mDBMS.Common.Data; // PENTING: Agar tipe Row dikenali

namespace mDBMS.StorageManager
{
    public static class DataReader
    {
        private const int BlockSize = 4096;
        private const int FileHeaderSize = 4096; // Sesuaikan dengan StorageEngine

        public static void ReadFile(string tableName)
        {
            string filePath = $"{tableName.ToLower()}.dat";
            
            // Cek di folder saat ini (output build)
            if (!File.Exists(filePath))
            {
                // Coba cek di folder bin/debug jika dijalankan dari root
                string alternativePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, filePath);
                if (File.Exists(alternativePath))
                {
                    filePath = alternativePath;
                }
                else
                {
                    Console.WriteLine($"[ERROR] File {filePath} tidak ditemukan.");
                    return;
                }
            }

            Console.WriteLine($"=== Membaca File: {filePath} ===");
            
            // Dapatkan skema (Hardcoded sementara untuk Milestone 2 karena Header File mungkin masih dummy/kosong)
            TableSchema schema = GetSchemaForTable(tableName);

            using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                // 1. Baca/Skip Header
                if (fs.Length <= FileHeaderSize)
                {
                    Console.WriteLine("File hanya berisi header atau kosong.");
                    return;
                }

                // Skip 4096 byte pertama (Metadata Header)
                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                Console.WriteLine($"[INFO] Melewati Header Metadata ({FileHeaderSize} bytes)...");

                // 2. Baca Blok Data
                byte[] buffer = new byte[BlockSize];
                int bytesRead;
                int blockIndex = 0;

                while ((bytesRead = fs.Read(buffer, 0, BlockSize)) > 0)
                {
                    if (bytesRead < BlockSize) Array.Clear(buffer, bytesRead, BlockSize - bytesRead);

                    Console.WriteLine($"\n--- Blok {blockIndex} ---");
                    
                    try 
                    {
                        // Deserialisasi menggunakan BlockSerializer yang baru
                        List<Row> rows = BlockSerializer.DeserializeBlock(schema, buffer);

                        if (rows.Count == 0)
                        {
                            Console.WriteLine("(Blok Kosong atau hanya berisi Slot Directory)");
                        }
                        else
                        {
                            int rowIndex = 0;
                            foreach (var row in rows)
                            {
                                PrintRow(row, rowIndex++);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Gagal membaca blok: {ex.Message}");
                    }

                    blockIndex++;
                }
            }
            Console.WriteLine("\n=== Pembacaan Selesai ===");
        }

        // Helper untuk mencetak Row (Pengganti Dictionary)
        private static void PrintRow(Row row, int index)
        {
            Console.Write($"[{index}] ");
            foreach (var col in row.Columns)
            {
                Console.Write($"{col.Key}: {col.Value} | ");
            }
            Console.WriteLine();
        }

        // Helper Skema (Sama seperti di StorageEngine & Seeder)
        // Ini diperlukan agar BlockSerializer tahu cara memotong byte
        private static TableSchema GetSchemaForTable(string tableName)
        {
            var schema = new TableSchema { TableName = tableName };

            if (tableName.Equals("Students", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "StudentID", Type = DataType.Int, Length = 4 });
                schema.Columns.Add(new ColumnSchema { Name = "FullName", Type = DataType.String, Length = 50 });
                // Tambahkan GPA jika di Seeder/RowSerializer sudah di-uncomment
                // schema.Columns.Add(new ColumnSchema { Name = "GPA", Type = DataType.Float, Length = 4 });
            }
            else if (tableName.Equals("Courses", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "CourseID", Type = DataType.Int, Length = 4 });
                schema.Columns.Add(new ColumnSchema { Name = "CourseName", Type = DataType.String, Length = 50 });
                schema.Columns.Add(new ColumnSchema { Name = "Credits", Type = DataType.Int, Length = 4 });
            }
            else if (tableName.Equals("Enrollments", StringComparison.OrdinalIgnoreCase))
            {
                schema.Columns.Add(new ColumnSchema { Name = "EnrollmentID", Type = DataType.Int, Length = 4 });
                schema.Columns.Add(new ColumnSchema { Name = "StudentID", Type = DataType.Int, Length = 4 });
                schema.Columns.Add(new ColumnSchema { Name = "CourseID", Type = DataType.Int, Length = 4 });
                schema.Columns.Add(new ColumnSchema { Name = "Grade", Type = DataType.String, Length = 2 });
            }

            return schema;
        }
    }
}