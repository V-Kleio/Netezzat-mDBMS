using System.Collections.Generic;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;

namespace mDBMS.QueryProcessorDriver.Mocks;

public class MockStorageManager : IStorageManager
{
    private readonly TestObserver _observer;
    private readonly Dictionary<string, List<Row>> _tables = new();

    public MockStorageManager(TestObserver observer)
    {
        _observer = observer;
        // Pre-populate mock data with non-null IDs.
        _tables["users"] = new List<Row>
        {
            new Row { id = "1", Columns = { ["name"] = "Alice" } },
            new Row { id = "2", Columns = { ["name"] = "Bob" } },
            new Row { id = "3", Columns = { ["name"] = "Charlie" } }
        };
    }

    public IEnumerable<Row> ReadBlock(DataRetrieval request)
    {
        _observer.Record($"StorageManager.ReadBlock(Table={request.Table})");
        
        if (_tables.TryGetValue(request.Table, out var tableData))
        {
            return tableData;
        }

        return Enumerable.Empty<Row>();
    }
    
    public int WriteBlock(DataWrite request)
    {
        _observer.Record($"StorageManager.WriteBlock(Table={request.Table})");
        return 1; // Simulate 1 row affected
    }

    public int DeleteBlock(DataDeletion request)
    {
        _observer.Record($"StorageManager.DeleteBlock(Table={request.Table})");
        return 1; // Simulate 1 row deleted
    }

    // Other IStorageManager methods can be mocked here if needed
    public Statistic GetStats(string tableName) { _observer.Record("StorageManager.GetStats"); return new Statistic(); }
    public void SetIndex(string table, string column, IndexType type) { _observer.Record("StorageManager.SetIndex"); }
    public int AddBlock(DataWrite request) { _observer.Record("StorageManager.AddBlock"); return 1; }
    public int WriteDisk(mDBMS.Common.Data.Page page) { _observer.Record("StorageManager.WriteDisk"); return 1; }
}
