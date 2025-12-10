using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class FinalSeeder
    {
        private static readonly Random _random = new Random(100); // Fixed seed agar hasil konsisten

        public static void Run(string dataPath = "")
        {
            if (string.IsNullOrEmpty(dataPath))
            {
                dataPath = AppDomain.CurrentDomain.BaseDirectory;
            }

            Console.WriteLine("==============================================");
            Console.WriteLine("       mDBMS Final Project Seeder");
            Console.WriteLine("==============================================");
            Console.WriteLine($"Target Directory: {dataPath}");
            Console.WriteLine();

            if (!Directory.Exists(dataPath))
            {
                Directory.CreateDirectory(dataPath);
            }

            CleanupExistingFiles(dataPath);

            // Generate data sesuai spesifikasi PDF
            SeedStudent(dataPath);
            SeedCourse(dataPath);
            SeedAttends(dataPath);

            Console.WriteLine();
            Console.WriteLine("Final Seeder completed successfully!");
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
        private static void SeedStudent(string dataPath)
        {
            // Sesuai PDF: StudentID (PK), FullName, GPA
            var schema = new TableSchema
            {
                TableName = "Student",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName", Type = DataType.String, Length = 50 },
                    new() { Name = "GPA", Type = DataType.Float, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "student.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] firstNames = { "Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kevin", "Luna", "Mike", "Nina", "Oscar" };
            string[] lastNames = { "Johnson", "Smith", "Brown", "Davis", "Wilson", "Evans", "Harris", "Clark", "Lewis", "Walker", "Hall", "Allen", "Young", "King" };

            List<byte[]> rows = new();
            int rowCount = 60; // Minimal 50 baris sesuai permintaan

            for (int i = 1; i <= rowCount; i++)
            {
                var row = new Row { id = i.ToString() }; // Row ID sederhana untuk internal
                row.Columns["StudentID"] = i;
                
                string fname = firstNames[_random.Next(firstNames.Length)];
                string lname = lastNames[_random.Next(lastNames.Length)];
                row.Columns["FullName"] = $"{fname} {lname}";
                
                // Random GPA 2.0 - 4.0
                double gpa = 2.0 + (_random.NextDouble() * 2.0);
                row.Columns["GPA"] = (float)Math.Round(gpa, 2);

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Student: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Course Table
        private static void SeedCourse(string dataPath)
        {
            // Sesuai PDF: CourseID (PK), Year, CourseName, "Course Description" (kita pakai CourseDescription)
            var schema = new TableSchema
            {
                TableName = "Course",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Year", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseName", Type = DataType.String, Length = 50 },
                    new() { Name = "CourseDescription", Type = DataType.String, Length = 200 } // LongText simulasi string panjang
                }
            };

            string filePath = Path.Combine(dataPath, "course.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            var subjects = new[] { "Databases", "Algorithms", "Operating Systems", "Network Security", "AI", "Machine Learning", "Web Dev", "Mobile Apps", "Cloud Computing", "Data Science" };
            var types = new[] { "Intro to", "Advanced", "Applied", "Principles of", "Fundamentals of" };

            List<byte[]> rows = new();
            int rowCount = 50; // Minimal 50 baris

            // Masukkan data sample wajib dari PDF
            var sampleCourses = new List<(int, int, string, string)>
            {
                (101, 2024, "Introduction to Databases", "A foundational course on database systems and SQL."),
                (102, 2024, "Data Structures", "An in-depth course on algorithms and data structures."),
                (103, 2023, "Operating Systems", "A course on operating system concepts.")
            };

            // Tambahkan sample courses dulu
            foreach(var c in sampleCourses)
            {
                var row = new Row { id = c.Item1.ToString() };
                row.Columns["CourseID"] = c.Item1;
                row.Columns["Year"] = c.Item2;
                row.Columns["CourseName"] = c.Item3;
                row.Columns["CourseDescription"] = c.Item4;
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            // Generate sisanya random mulai ID 200
            for (int i = 0; i < (rowCount - sampleCourses.Count); i++)
            {
                int cid = 200 + i;
                var row = new Row { id = cid.ToString() };
                row.Columns["CourseID"] = cid;
                row.Columns["Year"] = _random.Next(2020, 2026);
                
                string subj = subjects[_random.Next(subjects.Length)];
                string type = types[_random.Next(types.Length)];
                row.Columns["CourseName"] = $"{type} {subj}";
                row.Columns["CourseDescription"] = $"Description for {subj} course covering key topics.";

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Course: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Attends Table
        private static void SeedAttends(string dataPath)
        {
            // Sesuai PDF: StudentID, CourseID (Composite PK)
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
            HashSet<string> uniquePairs = new();

            // Sample data dari PDF
            var samples = new[] { (1, 101), (1, 102), (2, 101), (3, 103) };
            foreach(var s in samples)
            {
                var row = new Row { id = $"{s.Item1}-{s.Item2}" };
                row.Columns["StudentID"] = s.Item1;
                row.Columns["CourseID"] = s.Item2;
                rows.Add(RowSerializer.SerializeRow(schema, row));
                uniquePairs.Add($"{s.Item1}-{s.Item2}");
            }

            // Generate random sisanya sampai 60 rows
            int targetCount = 60;
            while(rows.Count < targetCount)
            {
                int sid = _random.Next(1, 61); // Range StudentID (1-60)
                
                // CourseID campuran antara sample (101-103) dan generated (200-247)
                int cid;
                if (_random.NextDouble() < 0.3) 
                    cid = _random.Next(101, 104); 
                else 
                    cid = _random.Next(200, 247);

                string key = $"{sid}-{cid}";
                if (!uniquePairs.Contains(key))
                {
                    uniquePairs.Add(key);
                    var row = new Row { id = key };
                    row.Columns["StudentID"] = sid;
                    row.Columns["CourseID"] = cid;
                    rows.Add(RowSerializer.SerializeRow(schema, row));
                }
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Attends: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Helper Methods (Sama seperti Seeder.cs)
        private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            List<byte[]> currentBlock = new();
            int currentSize = 4; // Header blok 2 bytes count + 2 bytes pointer

            foreach (var rowBytes in rows)
            {
                // Cek overflow (+2 bytes untuk pointer directory)
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
        #endregion
    }
}