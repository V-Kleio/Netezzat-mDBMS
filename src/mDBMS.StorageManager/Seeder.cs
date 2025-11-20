using System;
using System.Collections.Generic;
using System.IO;
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
            Console.WriteLine("Seeder selesai! Data dummy berhasil digenerate.");
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

            // Absolute path di folder bin/debug
            string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "students.dat");
            
            SchemaSerializer.WriteSchema(filePath, schema); 

            List<byte[]> rows = new();
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row(); 
                row["StudentID"] = i;
                row["FullName"] = $"Student {i}";
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }
            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Tabel Students: 50 baris -> {filePath}");
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
            string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "courses.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            string[] courseNames = { "Math", "Physics", "Chemistry", "Bio", "History" };
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row();
                row["CourseID"] = i;
                row["CourseName"] = $"{courseNames[i % 5]} {i}";
                row["Credits"] = 3;
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }
            WriteRowsToBlocks(filePath, rows);
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
            string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "enrollments.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            for (int i = 1; i <= 50; i++)
            {
                var row = new Row();
                row["EnrollmentID"] = i;
                row["StudentID"] = i;
                row["CourseID"] = i;
                row["Grade"] = "A";
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }
            WriteRowsToBlocks(filePath, rows);
        }

        private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            // Append Only karena Header sudah dibuat
            List<byte[]> currentBlock = new();
            int currentSize = 4; 

            foreach (var rowBytes in rows)
            {
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