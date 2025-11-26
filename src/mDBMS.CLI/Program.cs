using mDBMS.QueryProcessor;
using mDBMS.Common.Transaction;
using mDBMS.QueryOptimizer;
using mDBMS.StorageManager;
using mDBMS.ConcurrencyControl;
using mDBMS.FailureRecovery;

var storageManager = new StorageEngine();
var optimizer = new QueryOptimizerEngine(storageManager);
var concurrencyControl = new ConcurrencyControlManager();
var failureRecovery = new FailureRecoveryManager(null, storageManager);
var queryProcessor = new QueryProcessor(storageManager, optimizer, concurrencyControl, failureRecovery);

Console.WriteLine("mDBMS CLI siap digunakan. Ketik EXIT untuk keluar.");

while (true)
{
    Console.Write("mDBMS > ");
    var input = Console.ReadLine();

    if (input is null)
    {
        break;
    }

    if (string.Equals(input.Trim(), "EXIT", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine("Sampai jumpa!");
        break;
    }

    var result = queryProcessor.ExecuteQuery(input);
    PrintResult(result);
}

static void PrintResult(ExecutionResult result)
{
    var status = result.Success ? "SUCCESS" : "ERROR";
    Console.WriteLine($"[{status}] {result.Message}");

    if (result.Data != null)
    {
        Console.WriteLine("\nHasil:");
        foreach (var row in result.Data)
        {
            Console.WriteLine(string.Join(" | ", row.Columns.Select(kv => $"{kv.Key}: {kv.Value}")));
        }
        Console.WriteLine();
    }
}
