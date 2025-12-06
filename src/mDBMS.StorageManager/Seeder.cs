using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class Seeder
    {
        private static readonly Random _random = new Random(42); // Fixed seed for reproducibility

        public static void RunSeeder(string dataPath = "")
        {
            if (string.IsNullOrEmpty(dataPath))
            {
                dataPath = AppDomain.CurrentDomain.BaseDirectory;
            }

            Console.WriteLine("==============================================");
            Console.WriteLine("mDBMS Database Seeder");
            Console.WriteLine("==============================================");
            Console.WriteLine($"Target Directory: {dataPath}");
            Console.WriteLine();

            CleanupExistingFiles(dataPath);

            SeedStudents(dataPath);
            SeedCourses(dataPath);
            SeedEnrollments(dataPath);
            SeedDepartments(dataPath);
            SeedInstructors(dataPath);

            Console.WriteLine();
            Console.WriteLine("==============================================");
            Console.WriteLine("Seeder completed successfully!");
            Console.WriteLine("==============================================");
        }

        private static void CleanupExistingFiles(string dataPath)
        {
            string[] tables = { "students.dat", "courses.dat", "enrollments.dat",
                               "departments.dat", "instructors.dat" };

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

        #region Students Table
        private static void SeedStudents(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Students",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName", Type = DataType.String, Length = 50 },
                    new() { Name = "Email", Type = DataType.String, Length = 50 },
                    new() { Name = "Age", Type = DataType.Int, Length = 4 },
                    new() { Name = "GPA", Type = DataType.Float, Length = 4 },
                    new() { Name = "DepartmentID", Type = DataType.Int, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "students.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] firstNames = { "John", "Jane", "Michael", "Emily", "David", "Sarah",
                                   "James", "Emma", "Robert", "Olivia", "William", "Ava",
                                   "Richard", "Isabella", "Joseph", "Sophia", "Thomas", "Mia",
                                   "Charles", "Charlotte", "Daniel", "Amelia", "Matthew", "Harper" };

            string[] lastNames = { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                                  "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                                  "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore" };

            List<byte[]> rows = new();
            int studentCount = 100;

            for (int i = 1; i <= studentCount; i++)
            {
                var row = new Row();
                row.Columns["StudentID"] = i;

                string firstName = firstNames[_random.Next(firstNames.Length)];
                string lastName = lastNames[_random.Next(lastNames.Length)];
                row.Columns["FullName"] = $"{firstName} {lastName}";

                row.Columns["Email"] = $"{firstName.ToLower()}.{lastName.ToLower()}{i}@university.edu";
                row.Columns["Age"] = _random.Next(18, 26);
                row.Columns["GPA"] = Math.Round(2.0 + _random.NextDouble() * 2.0, 2); // GPA 2.0-4.0
                row.Columns["DepartmentID"] = _random.Next(1, 6); // 5 departments

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Students: {studentCount} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Courses Table
        private static void SeedCourses(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Courses",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseName", Type = DataType.String, Length = 100 },
                    new() { Name = "Credits", Type = DataType.Int, Length = 4 },
                    new() { Name = "DepartmentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "InstructorID", Type = DataType.Int, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "courses.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            var coursesByDept = new Dictionary<int, string[]>
            {
                { 1, new[] { "Introduction to Programming", "Data Structures", "Algorithms",
                            "Database Systems", "Operating Systems", "Computer Networks",
                            "Software Engineering", "Artificial Intelligence", "Machine Learning" } },
                { 2, new[] { "Calculus I", "Calculus II", "Linear Algebra", "Differential Equations",
                            "Probability Theory", "Statistics", "Discrete Mathematics", "Number Theory" } },
                { 3, new[] { "General Physics I", "General Physics II", "Quantum Mechanics",
                            "Thermodynamics", "Electromagnetism", "Modern Physics" } },
                { 4, new[] { "General Chemistry", "Organic Chemistry", "Inorganic Chemistry",
                            "Physical Chemistry", "Analytical Chemistry", "Biochemistry" } },
                { 5, new[] { "World History", "American History", "European History",
                            "Ancient Civilizations", "Modern History", "Economic History" } }
            };

            List<byte[]> rows = new();
            int courseId = 1;
            int[] credits = { 2, 3, 4 };

            foreach (var dept in coursesByDept)
            {
                foreach (var courseName in dept.Value)
                {
                    var row = new Row();
                    row.Columns["CourseID"] = courseId;
                    row.Columns["CourseName"] = courseName;
                    row.Columns["Credits"] = credits[_random.Next(credits.Length)];
                    row.Columns["DepartmentID"] = dept.Key;
                    row.Columns["InstructorID"] = _random.Next(1, 21); // 20 instructors

                    rows.Add(RowSerializer.SerializeRow(schema, row));
                    courseId++;
                }
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Courses: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Enrollments Table
        private static void SeedEnrollments(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Enrollments",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "EnrollmentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "StudentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CourseID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Grade", Type = DataType.String, Length = 2 },
                    new() { Name = "Semester", Type = DataType.String, Length = 20 }
                }
            };

            string filePath = Path.Combine(dataPath, "enrollments.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            List<byte[]> rows = new();
            string[] grades = { "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F" };
            string[] semesters = { "Fall 2023", "Spring 2024", "Fall 2024" };

            int enrollmentId = 1;
            int studentCount = 100;
            int courseCount = 50; // Approximate from courses

            for (int studentId = 1; studentId <= studentCount; studentId++)
            {
                int coursesToEnroll = _random.Next(3, 7);
                HashSet<int> enrolledCourses = new HashSet<int>();

                for (int i = 0; i < coursesToEnroll; i++)
                {
                    int courseId = _random.Next(1, courseCount + 1);

                    if (enrolledCourses.Contains(courseId))
                        continue;

                    enrolledCourses.Add(courseId);

                    var row = new Row();
                    row.Columns["EnrollmentID"] = enrollmentId;
                    row.Columns["StudentID"] = studentId;
                    row.Columns["CourseID"] = courseId;
                    row.Columns["Grade"] = grades[_random.Next(grades.Length)];
                    row.Columns["Semester"] = semesters[_random.Next(semesters.Length)];

                    rows.Add(RowSerializer.SerializeRow(schema, row));
                    enrollmentId++;
                }
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Enrollments: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Departments Table
        private static void SeedDepartments(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Departments",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "DepartmentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "DepartmentName", Type = DataType.String, Length = 50 },
                    new() { Name = "Building", Type = DataType.String, Length = 30 },
                    new() { Name = "Budget", Type = DataType.Float, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "departments.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            var departments = new[]
            {
                ("Computer Science", "Tech Building", 500000.0f),
                ("Mathematics", "Science Hall", 350000.0f),
                ("Physics", "Science Hall", 400000.0f),
                ("Chemistry", "Lab Complex", 450000.0f),
                ("History", "Liberal Arts", 250000.0f)
            };

            List<byte[]> rows = new();
            for (int i = 0; i < departments.Length; i++)
            {
                var row = new Row();
                row.Columns["DepartmentID"] = i + 1;
                row.Columns["DepartmentName"] = departments[i].Item1;
                row.Columns["Building"] = departments[i].Item2;
                row.Columns["Budget"] = departments[i].Item3;

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Departments: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }
        #endregion

        #region Instructors Table
        private static void SeedInstructors(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Instructors",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "InstructorID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName", Type = DataType.String, Length = 50 },
                    new() { Name = "Email", Type = DataType.String, Length = 50 },
                    new() { Name = "DepartmentID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Salary", Type = DataType.Float, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "instructors.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] titles = { "Dr.", "Prof." };
            string[] firstNames = { "Alice", "Bob", "Carol", "David", "Eve", "Frank",
                                   "Grace", "Henry", "Ivy", "Jack" };
            string[] lastNames = { "Anderson", "Brown", "Clark", "Davis", "Evans",
                                  "Foster", "Garcia", "Harris", "Irving", "Jackson" };

            List<byte[]> rows = new();
            int instructorCount = 20;

            for (int i = 1; i <= instructorCount; i++)
            {
                var row = new Row();
                row.Columns["InstructorID"] = i;

                string title = titles[_random.Next(titles.Length)];
                string firstName = firstNames[_random.Next(firstNames.Length)];
                string lastName = lastNames[_random.Next(lastNames.Length)];

                row.Columns["FullName"] = $"{title} {firstName} {lastName}";
                row.Columns["Email"] = $"{firstName.ToLower()}.{lastName.ToLower()}@university.edu";
                row.Columns["DepartmentID"] = _random.Next(1, 6);
                row.Columns["Salary"] = (float)(60000 + _random.Next(40000)); // $60k-$100k

                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Instructors: {rows.Count} rows -> {Path.GetFileName(filePath)}");
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
