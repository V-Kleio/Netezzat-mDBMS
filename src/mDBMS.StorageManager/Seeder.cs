using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class Seeder
    {
        private static readonly Random _random = new Random(12345);

        public static void RunSeeder(string dataPath = "")
        {
            if (string.IsNullOrEmpty(dataPath))
            {
                dataPath = AppDomain.CurrentDomain.BaseDirectory;
            }

            Console.WriteLine("==============================================");
            Console.WriteLine("mDBMS Database Seeder (50 Rows Generator)");
            Console.WriteLine("==============================================");
            Console.WriteLine($"Target Directory: {dataPath}");
            Console.WriteLine();

            CleanupExistingFiles(dataPath);
            int count = 50;

            SeedStudents(dataPath, count);
            SeedCourses(dataPath, count);
            SeedAttends(dataPath, count);

            Console.WriteLine();
            Console.WriteLine("==============================================");
            Console.WriteLine("Seeder completed successfully!");
            Console.WriteLine("==============================================");
        }

        private static void CleanupExistingFiles(string dataPath)
        {
            string[] tables = { "student.dat", "course.dat", "attends.dat" };

            foreach (var table in tables)
            {
                string filePath = Path.Combine(dataPath, table);
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                    Console.WriteLine($"Deleted existing: {table}");
                }
            }
            Console.WriteLine();
        }

        #region Student Table
        private static void SeedStudents(string dataPath, int count)
        {
            var schema = new TableSchema
            {
                TableName = "Student",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName", Type = DataType.String, Length = 50 },
                    new() { Name = "GPA", Type = DataType.Float, Length = 4 },
                }
            };

            string filePath = Path.Combine(dataPath, "student.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] firstNames = { "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Mallory", "Niaj", "Oscar", "Peggy", "Sybil", "Trent", "Walter" };
            string[] lastNames = { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez" };

            List<byte[]> rows = new();

            for (int i = 1; i <= count; i++)
            {
                var row = new Row();
                
                row.Columns["StudentID"] = i;
                string fname = firstNames[_random.Next(firstNames.Length)];
                string lname = lastNames[_random.Next(lastNames.Length)];
                row.Columns["FullName"] = $"{fname} {lname}";
                
                // GPA: Range 2.00 sampai 4.00
                float gpa = (float)Math.Round(2.0 + (_random.NextDouble() * 2.0), 2);
                row.Columns["GPA"] = gpa;

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Student: {rows.Count} rows generated -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Course Table
        private static void SeedCourses(string dataPath, int count)
        {
            var schema = new TableSchema
            {
                TableName = "Course",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Year", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseName", Type = DataType.String, Length = 50 },
                    new() { Name = "CourseDescription", Type = DataType.String, Length = 255 } 
                }
            };

            string filePath = Path.Combine(dataPath, "course.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] subjects = { "Databases", "Algorithms", "OS", "Networks", "Calculus", "Physics", "AI", "Security", "Web Dev", "Cloud" };
            string[] levels = { "Intro to", "Advanced", "Applied", "Principles of", "Fundamentals of" };

            List<byte[]> rows = new();

            for (int i = 1; i <= count; i++)
            {
                var row = new Row();
                
                // CourseID: 101 sampai 150
                row.Columns["CourseID"] = 100 + i; 
                
                // Year: 2020 - 2024
                row.Columns["Year"] = _random.Next(2020, 2025);
                
                // CourseName
                string subj = subjects[_random.Next(subjects.Length)];
                string lvl = levels[_random.Next(levels.Length)];
                // Agar unik, kita tambahkan ID di nama jika perlu, atau andalkan kombinasi random
                // Disini kita format simple agar terlihat seperti nama matkul asli
                row.Columns["CourseName"] = $"{lvl} {subj} {i}"; 

                // Description
                row.Columns["CourseDescription"] = $"This course covers various topics regarding {subj}. Designed for student cohort {i}.";

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Course: {rows.Count} rows generated -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Attends Table
        private static void SeedAttends(string dataPath, int count)
        {
            var schema = new TableSchema
            {
                TableName = "Attends",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "attends.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            
            // Set untuk memastikan Primary Key (StudentID, CourseID) unik
            HashSet<string> existingPairs = new HashSet<string>();
            
            int generated = 0;
            // Kita generate 50 row relasi
            while (generated < count)
            {
                // Ambil Random StudentID (1-50)
                int sId = _random.Next(1, 51);
                
                // Ambil Random CourseID (101-150) - Sesuai loop Course diatas
                int cId = 100 + _random.Next(1, 51);

                string key = $"{sId}-{cId}";

                // Cek duplikasi PK
                if (!existingPairs.Contains(key))
                {
                    existingPairs.Add(key);

                    var row = new Row();
                    row.Columns["StudentID"] = sId;
                    row.Columns["CourseID"] = cId;

                    rows.Add(RowSerializer.SerializeRow(schema, row));
                    generated++;
                }
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Attends: {rows.Count} rows generated -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Helper Methods
        private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            List<byte[]> currentBlock = new();
            int currentSize = 4; // RecordCount 2 bytes + reserve

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
                currentSize += rowBytes.Length + 2; // +2 for directory entry
            }

            if (currentBlock.Count > 0)
            {
                var block = BlockSerializer.CreateBlock(currentBlock);
                BlockSerializer.AppendBlockToFile(filePath, block);
            }
        }
        #endregion
    }
}