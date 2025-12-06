using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
using mDBMS.QueryOptimizer;
using mDBMS.StorageManager;

namespace mDBMS.QueryOptimizerDriver;

/// <summary>
/// Driver untuk testing Query Optimizer.
/// Mendemonstrasikan:
/// 1. Parsing SQL query
/// 2. Building plan tree
/// 3. Cost calculation
/// 4. Plan visualization
/// </summary>
class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("=== mDBMS Query Optimizer Driver ===\n");

        // Setup real storage engine (tanpa buffer manager untuk testing)
        var storageEngine = new StorageEngine(null);
        
        // Wrap dengan StorageManagerWrapper untuk statistik aktual
        var storageManager = new StorageManagerWrapper(storageEngine);
        
        // Buat optimizer engine
        var optimizer = new QueryOptimizerEngine(storageManager);

        // Test cases
        // Test dengan tabel aktual dari data/ folder
        RunTestCase(optimizer, "Simple SELECT * dari students",
            "SELECT * FROM students");

        RunTestCase(optimizer, "SELECT dengan WHERE pada students",
            "SELECT id, name FROM students WHERE id > 5");

        RunTestCase(optimizer, "SELECT dengan ORDER BY pada courses",
            "SELECT name, credits FROM courses ORDER BY credits DESC");

        RunTestCase(optimizer, "Complex query pada enrollments",
            "SELECT student_id, course_id FROM enrollments WHERE grade > 70 ORDER BY grade ASC");
        RunTestCase(optimizer, "JOIN antara students dan enrollments",
            "SELECT students.name, enrollments.course_id FROM students INNER JOIN enrollments ON students.id = enrollments.student_id WHERE enrollments.grade >= 80 ORDER BY students.name ASC");

        // Run edge case tests
        Console.WriteLine("\n\n");
        EdgeCaseTester.RunAllTests(optimizer);

        Console.WriteLine("\n=== Test Selesai ===");
    }

    static void RunTestCase(QueryOptimizerEngine optimizer, string testName, string sql)
    {
        Console.WriteLine($"\n--- {testName} ---");
        Console.WriteLine($"SQL: {sql}");
        Console.WriteLine();

        try
        {
            // Step 1: Parse query
            var query = optimizer.ParseQuery(sql);
            Console.WriteLine($"Parsed successfully");
            Console.WriteLine($"  Table: {query.Table}");
            Console.WriteLine($"  Columns: {string.Join(", ", query.SelectedColumns)}");
            if (!string.IsNullOrWhiteSpace(query.WhereClause))
                Console.WriteLine($"  WHERE: {query.WhereClause}");
            if (query.OrderBy != null && query.OrderBy.Any())
                Console.WriteLine($"  ORDER BY: {string.Join(", ", query.OrderBy.Select(o => $"{o.Column} {(o.IsAscending ? "ASC" : "DESC")}"))}");

            // Step 2: Optimize query (get QueryPlan with embedded tree)
            var queryPlan = optimizer.OptimizeQuery(query);
            Console.WriteLine($"\nOptimized successfully");
            
            // Step 3: Verify PlanTree is embedded
            if (queryPlan.PlanTree != null)
            {
                Console.WriteLine("QueryPlan contains PlanTree");
            }
            
            // Step 4: Print plan tree
            Console.WriteLine("\nExecution Plan Tree:");
            PrintPlanTree(queryPlan.PlanTree!, 0);

            // Step 5: Print costs
            Console.WriteLine($"\nEstimated Rows: {queryPlan.PlanTree!.EstimatedRows:F0}");
            Console.WriteLine($"Total Cost: {queryPlan.PlanTree.TotalCost:F2}");

        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error: {ex.Message}");
            Console.WriteLine($"  {ex.GetType().Name}");
        }
    }

    static void PrintPlanTree(PlanNode node, int depth)
    {
        string indent = new string(' ', depth * 2);
        Console.WriteLine($"{indent}└─ {node}");

        // Recursively print children based on node type
        switch (node)
        {
            case FilterNode filterNode:
                PrintPlanTree(filterNode.Input, depth + 1);
                break;
            case ProjectNode projectNode:
                PrintPlanTree(projectNode.Input, depth + 1);
                break;
            case SortNode sortNode:
                PrintPlanTree(sortNode.Input, depth + 1);
                break;
            case AggregateNode aggNode:
                PrintPlanTree(aggNode.Input, depth + 1);
                break;
            case JoinNode joinNode:
                PrintPlanTree(joinNode.Left, depth + 1);
                PrintPlanTree(joinNode.Right, depth + 1);
                break;
            // Leaf nodes (TableScan, IndexScan, IndexSeek) have no children
        }
    }
}

/// <summary>
/// Wrapper untuk StorageEngine yang menyediakan statistik aktual dari file .dat
/// </summary>
class StorageManagerWrapper : IStorageManager
{
    private readonly StorageEngine _storageEngine;
    private static readonly string DataPath = GetDataPath();
    private const int BlockSize = 4096;
    private const int FileHeaderSize = 4096;

    public StorageManagerWrapper(StorageEngine storageEngine)
    {
        _storageEngine = storageEngine;
    }

    private static string GetDataPath()
    {
        string currentDir = Directory.GetCurrentDirectory();
        DirectoryInfo? dir = new DirectoryInfo(currentDir);

        while (dir != null)
        {
            if (dir.GetFiles("*.sln").Length > 0)
            {
                return Path.Combine(dir.FullName, "data");
            }
            dir = dir.Parent;
        }

        string? customPath = Environment.GetEnvironmentVariable("MDBMS_DATA_PATH");
        if (!string.IsNullOrEmpty(customPath))
        {
            return customPath;
        }

        return Path.Combine(currentDir, "data");
    }

    public Statistic GetStats(string tableName)
    {
        string fileName = $"{tableName.ToLower()}.dat";
        string fullPath = Path.Combine(DataPath, fileName);

        if (!File.Exists(fullPath))
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist at {fullPath}");
        }

        // Baca statistik aktual dari file
        var schema = SchemaSerializer.ReadSchema(fullPath);
        var fileInfo = new FileInfo(fullPath);
        
        // Hitung jumlah block
        int blockCount = (int)((fileInfo.Length - FileHeaderSize) / BlockSize);
        if (blockCount < 0) blockCount = 0;

        // Hitung estimasi tuple count dengan membaca beberapa block
        int tupleCount = 0;
        int sampleBlockCount = Math.Min(blockCount, 5); // Sample max 5 block
        
        if (sampleBlockCount > 0)
        {
            using var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read);
            int totalRowsInSample = 0;
            
            for (int i = 0; i < sampleBlockCount; i++)
            {
                byte[] blockData = new byte[BlockSize];
                fs.Seek(FileHeaderSize + (i * BlockSize), SeekOrigin.Begin);
                int bytesRead = fs.Read(blockData, 0, BlockSize);
                
                if (bytesRead == BlockSize)
                {
                    var rows = BlockSerializer.DeserializeBlock(schema, blockData);
                    totalRowsInSample += rows.Count;
                }
            }
            
            // Estimasi total tuple berdasarkan sample
            int avgRowsPerBlock = sampleBlockCount > 0 ? totalRowsInSample / sampleBlockCount : 0;
            tupleCount = avgRowsPerBlock * blockCount;
            if (tupleCount == 0 && totalRowsInSample > 0) tupleCount = totalRowsInSample;
        }

        // Hitung ukuran tuple berdasarkan schema
        int tupleSize = 0;
        foreach (var col in schema.Columns)
        {
            tupleSize += col.Length > 0 ? col.Length : 4; // Default 4 bytes jika tidak ada length
        }

        // Blocking factor
        int blockingFactor = tupleSize > 0 ? BlockSize / tupleSize : 1;

        Console.WriteLine($"  [Stats] Table: {tableName}, Blocks: {blockCount}, EstTuples: {tupleCount}, TupleSize: {tupleSize}B");

        return new Statistic
        {
            Table = tableName,
            TupleCount = tupleCount,
            BlockCount = blockCount,
            TupleSize = tupleSize,
            BlockingFactor = blockingFactor,
            DistinctValues = Math.Max(1, tupleCount / 10), // Estimasi kasar
            Indices = new List<(string, IndexType)>()
        };
    }

    // Dihandle oleh StorageEngine
    public IEnumerable<Row> ReadBlock(DataRetrieval data_retrieval) => _storageEngine.ReadBlock(data_retrieval);
    public int WriteBlock(DataWrite data_write) => _storageEngine.WriteBlock(data_write);
    public int AddBlock(DataWrite data_write) => _storageEngine.AddBlock(data_write);
    public int DeleteBlock(DataDeletion data_deletion) => _storageEngine.DeleteBlock(data_deletion);
    public int WriteDisk(Page page) => _storageEngine.WriteDisk(page);
    public void SetIndex(string table, string column, IndexType type) => _storageEngine.SetIndex(table, column, type);
}
