// buat seedernya 

using System;
using System.Collections.Generic; 

namespace mDBMS.StorageManager
{
    public static class Seeder
    // ini buat untuk nge-seed data dummy ke file .dat
    {
        public static void RunSeeder()
        {
            // seed tabel Students
            SeedStudents();
            
            // seed tabel Courses
            SeedCourses();
            
            // seed tabel Enrollments
            SeedEnrollments();
            
            Console.WriteLine("Seeder selesai! Semua file .dat sudah dibuat dengan data dummy.");
        }

        private static void SeedStudents()
        {
            // definisi skema tabel Students
            var schema = new TableSchema
            {
                TableName = "Students",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName",  Type = DataType.String, Length = 50 }
                }
            };

            string filePath = "students.dat";

            // tulis header skema ke file
            SchemaSerializer.WriteSchema(filePath, schema);

            // generate 50 data dummy untuk Students
            List<byte[]> rows = new();
            for (int i = 1; i <= 50; i++)
            {
                var row = new Dictionary<string, object>
                {
                    ["StudentID"] = i,
                    ["FullName"] = $"Student {i}"
                };
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            // bagi data ke dalam blok-blok 4KB
            WriteRowsToBlocks(filePath, rows);

            Console.WriteLine($"Tabel Students: 50 baris berhasil ditulis ke {filePath}");
        }

        private static void SeedCourses()
        {
            // definisi skema tabel Courses
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

            string filePath = "courses.dat";

            // tulis header skema ke file
            SchemaSerializer.WriteSchema(filePath, schema);

            // generate 50 data dummy untuk Courses
            List<byte[]> rows = new();
            string[] courseNames = { "Mathematics", "Physics", "Chemistry", "Biology", "History", 
                                    "Geography", "English", "Programming", "Database", "Networks" };
            
            for (int i = 1; i <= 50; i++)
            {
                var row = new Dictionary<string, object>
                {
                    ["CourseID"] = i,
                    ["CourseName"] = $"{courseNames[i % courseNames.Length]} {((i-1) / courseNames.Length) + 1}",
                    ["Credits"] = (i % 4) + 2  // credits antara 2-5
                };
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            // bagi data ke dalam blok-blok 4KB
            WriteRowsToBlocks(filePath, rows);

            Console.WriteLine($"Tabel Courses: 50 baris berhasil ditulis ke {filePath}");
        }

        private static void SeedEnrollments()
        {
            // definisi skema tabel Enrollments
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

            string filePath = "enrollments.dat";

            // tulis header skema ke file
            SchemaSerializer.WriteSchema(filePath, schema);

            // generate 50 data dummy untuk Enrollments
            List<byte[]> rows = new();
            string[] grades = { "A", "A-", "B+", "B", "B-", "C+", "C" };
            
            for (int i = 1; i <= 50; i++)
            {
                var row = new Dictionary<string, object>
                {
                    ["EnrollmentID"] = i,
                    ["StudentID"] = ((i - 1) % 50) + 1,  // student ID 1-50
                    ["CourseID"] = ((i - 1) % 50) + 1,    // course ID 1-50
                    ["Grade"] = grades[i % grades.Length]
                };
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            // bagi data ke dalam blok-blok 4KB
            WriteRowsToBlocks(filePath, rows);

            Console.WriteLine($"Tabel Enrollments: 50 baris berhasil ditulis ke {filePath}");
        }

        private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            // bagi data ke blok-blok 4KB
            List<byte[]> currentBlock = new();
            int currentSize = 4;  // reserved untuk header blok (record count + directory offset)

            foreach (var rowBytes in rows)
            {
                // cek apakah row bisa masuk ke blok saat ini
                // +2 untuk slot directory entry
                if (currentSize + rowBytes.Length + 2 > BlockSerializer.BlockSize)
                {
                    // blok penuh, tulis blok saat ini ke file
                    var block = BlockSerializer.CreateBlock(currentBlock);
                    BlockSerializer.AppendBlockToFile(filePath, block);

                    // reset untuk blok baru
                    currentBlock.Clear();
                    currentSize = 4;
                }

                // tambahkan row ke blok saat ini
                currentBlock.Add(rowBytes);
                currentSize += rowBytes.Length + 2;  // +2 untuk slot directory
            }

            // tulis sisa blok terakhir jika ada
            if (currentBlock.Count > 0)
            {
                var block = BlockSerializer.CreateBlock(currentBlock);
                BlockSerializer.AppendBlockToFile(filePath, block);
            }
        }
    }
}