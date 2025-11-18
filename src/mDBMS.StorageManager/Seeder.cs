using System;
using System.Collections.Generic;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class Seeder
    {
        public static void RunSeeder()
        {
            SeedStudents();
            SeedCourses();
            SeedEnrollments();
            Console.WriteLine("Seeder selesai! Semua file .dat sudah dibuat dengan data dummy.");
        }

        private static void SeedStudents()
        {
            var schema = new TableSchema
            {
                TableName = "Students",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName",  Type = DataType.String, Length = 50 }
                }
            };

            string filePath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "students.dat");
            
            //SchemaSerializer.WriteSchema(filePath, schema); 

            List<byte[]> rows = new();
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row(); 
                row["StudentID"] = i;
                row["FullName"] = $"Student {i}";
                
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Tabel Students: 50 baris berhasil ditulis ke {filePath}");
        }

        private static void SeedCourses()
        {
            var schema = new TableSchema
            {
                TableName = "Courses",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseName", Type = DataType.String, Length = 50 },
                    new() { Name = "Credits", Type = DataType.Int, Length = 4 }
                }
            };

            string filePath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "courses.dat");
            //SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            string[] courseNames = { "Mathematics", "Physics", "Chemistry", "Biology", "History", 
                                    "Geography", "English", "Programming", "Database", "Networks" };
            
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row();
                row["CourseID"] = i;
                row["CourseName"] = $"{courseNames[i % courseNames.Length]} {((i-1) / courseNames.Length) + 1}";
                row["Credits"] = (i % 4) + 2;
                
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Tabel Courses: 50 baris berhasil ditulis ke {filePath}");
        }

        private static void SeedEnrollments()
        {
            var schema = new TableSchema
            {
                TableName = "Enrollments",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "EnrollmentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Grade", Type = DataType.String, Length = 2 }
                }
            };

            string filePath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "enrollments.dat");
            //SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            string[] grades = { "A", "A-", "B+", "B", "B-", "C+", "C" };
            
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row();
                row["EnrollmentID"] = i;
                row["StudentID"] = ((i - 1) % 50) + 1;
                row["CourseID"] = ((i - 1) % 50) + 1;
                row["Grade"] = grades[i % grades.Length];

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Tabel Enrollments: 50 baris berhasil ditulis ke {filePath}");
        }

       private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            // --- TULIS HEADER DUMMY JIKA FILE BARU ---
            // StorageEngine mengharapkan 4KB pertama adalah Metadata.
            // Karena SchemaSerializer belum aktif, isi 0 saja.
            if (!System.IO.File.Exists(filePath))
            {
                using (var fs = new System.IO.FileStream(filePath, System.IO.FileMode.Create, System.IO.FileAccess.Write))
                {
                    byte[] emptyHeader = new byte[4096]; // 4KB Header Kosong
                    fs.Write(emptyHeader, 0, emptyHeader.Length);
                }
            }
            // ----------------------------------------------------

            List<byte[]> currentBlock = new();
            int currentSize = 4; // Header (Count + DirOffset)

            foreach (var rowBytes in rows)
            {
                // +2 byte untuk pointer slot directory per record
                if (currentSize + rowBytes.Length + 2 > BlockSerializer.BlockSize)
                {
                    var block = BlockSerializer.CreateBlock(currentBlock);
                    BlockSerializer.AppendBlockToFile(filePath, block);

                    currentBlock.Clear();
                    currentSize = 4;
                }

                currentBlock.Add(rowBytes);
                currentSize += rowBytes.Length + 2;
            }

            if (currentBlock.Count > 0)
            {
                var block = BlockSerializer.CreateBlock(currentBlock);
                BlockSerializer.AppendBlockToFile(filePath, block);
            }
        }
    }
}