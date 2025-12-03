using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
using mDBMS.QueryOptimizer;

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

        // Setup mock storage manager
        var storageManager = new MockStorageManager();
        
        // Buat optimizer engine
        var optimizer = new QueryOptimizerEngine(storageManager);

        // Test cases
        RunTestCase(optimizer, "Simple SELECT *",
            "SELECT * FROM employees");

        RunTestCase(optimizer, "SELECT dengan WHERE",
            "SELECT id, name, salary FROM employees WHERE salary > 50000");

        RunTestCase(optimizer, "SELECT dengan ORDER BY",
            "SELECT name, age FROM employees ORDER BY age DESC");

        RunTestCase(optimizer, "Complex query dengan WHERE dan ORDER BY",
            "SELECT id, name, dept FROM employees WHERE age > 25 ORDER BY name ASC");

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
            Console.WriteLine($"✓ Parsed successfully");
            Console.WriteLine($"  Table: {query.Table}");
            Console.WriteLine($"  Columns: {string.Join(", ", query.SelectedColumns)}");
            if (!string.IsNullOrWhiteSpace(query.WhereClause))
                Console.WriteLine($"  WHERE: {query.WhereClause}");
            if (query.OrderBy != null && query.OrderBy.Any())
                Console.WriteLine($"  ORDER BY: {string.Join(", ", query.OrderBy.Select(o => $"{o.Column} {(o.IsAscending ? "ASC" : "DESC")}"))}");

            // Step 2: Optimize query (get QueryPlan with embedded tree)
            var queryPlan = optimizer.OptimizeQuery(query);
            Console.WriteLine($"\n✓ Optimized successfully");
            
            // Step 3: Verify PlanTree is embedded
            if (queryPlan.PlanTree != null)
            {
                Console.WriteLine("✓ QueryPlan contains PlanTree");
            }
            
            // Step 4: Print plan tree
            Console.WriteLine("\nExecution Plan Tree:");
            PrintPlanTree(queryPlan.PlanTree!, 0);

            // Step 5: Print costs
            Console.WriteLine($"\nEstimated Rows: {queryPlan.PlanTree!.EstimatedRows:F0}");
            Console.WriteLine($"Total Cost: {queryPlan.PlanTree.TotalCost:F2}");

            // Step 6: Test flat steps (backward compatibility)
            Console.WriteLine($"\nBackward Compatibility (Flat Steps):");
            Console.WriteLine($"  QueryPlan Steps Count: {queryPlan.Steps.Count}");
            Console.WriteLine($"  QueryPlan Total Cost: {queryPlan.TotalEstimatedCost:F2}");
            Console.WriteLine($"  Steps:");
            foreach (var step in queryPlan.Steps)
            {
                Console.WriteLine($"    {step.Order}. {step.Operation} - {step.Description}");
            }
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
/// Mock Storage Manager untuk testing tanpa database nyata.
/// </summary>
class MockStorageManager : IStorageManager
{
    public Statistic GetStats(string tableName)
    {
        // Return mock statistics untuk table "employees"
        if (tableName.Equals("employees", StringComparison.OrdinalIgnoreCase))
        {
            return new Statistic
            {
                TupleCount = 10000,
                BlockCount = 100,
                BlockingFactor = 100,
                DistinctValues = 1000,
                Indices = new List<(string, IndexType)>
                {
                    ("id", IndexType.BTree),
                    ("age", IndexType.BTree)
                }
            };
        }

        // Default stats untuk tabel lain
        return new Statistic
        {
            TupleCount = 1000,
            BlockCount = 10,
            BlockingFactor = 100,
            DistinctValues = 100,
            Indices = new List<(string, IndexType)>()
        };
    }

    // Implement required IStorageManager interface methods (not used in this driver, just stubs)
    public IEnumerable<Row> ReadBlock(DataRetrieval data_retrieval) => Enumerable.Empty<Row>();
    public int WriteBlock(DataWrite data_write) => 0;
    public int AddBlock(DataWrite data_write) => 0;
    public int DeleteBlock(DataDeletion data_deletion) => 0;
    public int WriteDisk(Page page) => 1;
    public void SetIndex(string table, string column, IndexType type) { }
}
