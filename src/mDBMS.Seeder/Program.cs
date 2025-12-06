using mDBMS.StorageManager;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("==============================================");
        Console.WriteLine("       mDBMS Database Seeder Tool");
        Console.WriteLine("==============================================");
        Console.WriteLine();

        string dataPath = GetDataPath(args);

        Console.WriteLine($"This will create/overwrite database files in:");
        Console.WriteLine($"  {dataPath}");
        Console.WriteLine();
        Console.Write("Continue? (y/n): ");

        string? response = Console.ReadLine();
        if (response?.ToLower() != "y")
        {
            Console.WriteLine("Seeding cancelled.");
            return;
        }

        Console.WriteLine();

        try
        {
            if (!Directory.Exists(dataPath))
            {
                Directory.CreateDirectory(dataPath);
            }

            Seeder.RunSeeder(dataPath);

            ShowDatabaseStats(dataPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

    static string GetDataPath(string[] args)
    {
        if (args.Length > 0)
        {
            return args[0];
        }

        return Path.Combine(Environment.CurrentDirectory, "data");
    }

    static void ShowDatabaseStats(string dataPath)
    {
        Console.WriteLine();
        Console.WriteLine("==============================================");
        Console.WriteLine("Database Statistics");
        Console.WriteLine("==============================================");

        string[] tables = { "students.dat", "courses.dat", "enrollments.dat",
                           "departments.dat", "instructors.dat" };

        long totalSize = 0;

        foreach (var table in tables)
        {
            string filePath = Path.Combine(dataPath, table);
            if (File.Exists(filePath))
            {
                FileInfo fi = new FileInfo(filePath);
                long sizeKB = fi.Length / 1024;
                totalSize += fi.Length;

                Console.WriteLine($"  {table,-20} {sizeKB,8} KB");
            }
        }

        Console.WriteLine($"  {"─────────────────────",-20} {"────────",8}");
        Console.WriteLine($"  {"Total",-20} {totalSize / 1024,8} KB");
        Console.WriteLine("==============================================");
    }
}
