using System;
using System.Collections.Generic;
using System.Linq;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.QueryProcessor;
using mDBMS.QueryProcessorDriver.Mocks;

namespace mDBMS.QueryProcessorDriver;

public class QueryProcessorDriver
{
    public static void Main(string[] args)
    {
        var observer = new TestObserver();
        
        var storageManager = new MockStorageManager(observer);
        var queryOptimizer = new MockQueryOptimizer(observer);
        var ccManager = new MockConcurrencyControlManager(observer);
        var frManager = new MockFailureRecoveryManager(observer);

        var queryProcessor = new mDBMS.QueryProcessor.QueryProcessor(storageManager, queryOptimizer, ccManager, frManager);

        TestSelectQuery(queryProcessor, observer, storageManager);
        TestInsertQuery(queryProcessor, observer, storageManager);
        TestBeginTransaction(queryProcessor, observer);
        TestCommitTransaction(queryProcessor, observer);
        TestAbortTransaction(queryProcessor, observer);
        TestInvalidQuery(queryProcessor, observer);

        observer.PrintSummary();
    }

    private static void TestSelectQuery(IQueryProcessor qp, TestObserver observer, IStorageManager storageManager)
    {
        observer.Reset();
        const string query = "SELECT * FROM users";
        
        var expectedEvents = new List<string>
        {
            "QueryOptimizer.ParseQuery(SQL='SELECT * FROM users')",
            "QueryOptimizer.OptimizeQuery(Table='users')",
            "StorageManager.ReadBlock(Table=users)",
            "ConcurrencyControlManager.ValidateObject",
            "ConcurrencyControlManager.ValidateObject",
            "ConcurrencyControlManager.ValidateObject",
            "StorageManager.ReadBlock(Table=users)",
            "RowValidation.Passed(RowId=1)",
            "RowValidation.Passed(RowId=2)",
            "RowValidation.Passed(RowId=3)"
        };
        
        qp.ExecuteQuery(query, 1);

        // After executing the query, validate rows returned by the storage manager
        var retrieval = new DataRetrieval("users", new string[] { "*" });
        var rows = storageManager.ReadBlock(retrieval);
        ValidateRows(rows, observer);

        observer.Verify("SELECT Query", expectedEvents);
    }
    
    private static void TestInsertQuery(IQueryProcessor qp, TestObserver observer, IStorageManager storageManager)
    {
        observer.Reset();
        const string query = "INSERT INTO users VALUES (1, 'name')";

        var expectedEvents = new List<string>
        {
            "QueryOptimizer.ParseQuery(SQL='INSERT INTO users VALUES (1, 'name')')",
            "QueryOptimizer.OptimizeQuery(Table='users')",
            "StorageManager.ReadBlock(Table=users)",
            "StorageManager.ReadBlock(Table=users)",
            "RowValidation.Passed(RowId=1)",
            "RowValidation.Passed(RowId=2)",
            "RowValidation.Passed(RowId=3)"
        };
        
        qp.ExecuteQuery(query, 1);
        // After insert, re-read storage to validate the inserted row(s)
        string table = "";
        if (query.Contains("INTO")) table = query.Split("INTO")[1].Trim().Split(' ')[0];
        var retrieval = new DataRetrieval(table, new string[] { "*" });
        var rows = storageManager.ReadBlock(retrieval);
        ValidateRows(rows, observer);

        observer.Verify("INSERT Query", expectedEvents);
    }

    private static void TestBeginTransaction(IQueryProcessor qp, TestObserver observer)
    {
        observer.Reset();
        const string query = "BEGIN TRANSACTION";

        var expectedEvents = new List<string>
        {
            "ConcurrencyControlManager.BeginTransaction"
        };
        
        qp.ExecuteQuery(query, -1);
        observer.Verify("BEGIN TRANSACTION", expectedEvents);
    }

    private static void ValidateRows(IEnumerable<Row> rows, TestObserver observer)
    {
        foreach (var row in rows)
        {
            if (row == null)
            {
                observer.Record("RowValidation.Failed(Row=null, Reason=NullRow)");
                continue;
            }

            if (string.IsNullOrWhiteSpace(row.id))
            {
                observer.Record($"RowValidation.Failed(RowId={row?.id ?? "<null>"}, Reason=MissingId)");
                continue;
            }

            if (row.Columns == null)
            {
                observer.Record($"RowValidation.Failed(RowId={row.id}, Reason=MissingColumns)");
                continue;
            }

            var nullCol = row.Columns.FirstOrDefault(kv => kv.Value == null);
            if (!EqualityComparer<KeyValuePair<string, object>>.Default.Equals(nullCol, default))
            {
                observer.Record($"RowValidation.Failed(RowId={row.id}, Reason=NullColumn:{nullCol.Key})");
                continue;
            }

            observer.Record($"RowValidation.Passed(RowId={row.id})");
        }
    }
    private static void TestCommitTransaction(IQueryProcessor qp, TestObserver observer)
    {
        observer.Reset();
        const string query = "COMMIT";
        
        var expectedEvents = new List<string>
        {
            "ConcurrencyControlManager.EndTransaction(ID=1, Commit=True)"
        };
        
        qp.ExecuteQuery(query, 1);
        observer.Verify("COMMIT TRANSACTION", expectedEvents);
    }

    private static void TestAbortTransaction(IQueryProcessor qp, TestObserver observer)
    {
        observer.Reset();
        const string query = "ABORT";
        
        var expectedEvents = new List<string>
        {
            "ConcurrencyControlManager.EndTransaction(ID=1, Commit=False)",
            "FailureRecoveryManager.UndoTransaction(ID=1)"
        };
        
        qp.ExecuteQuery(query, 1);
        observer.Verify("ABORT TRANSACTION", expectedEvents);
    }

    private static void TestInvalidQuery(IQueryProcessor qp, TestObserver observer)
    {
        observer.Reset();
        const string query = "INVALID SYNTAX";
        
        var expectedEvents = new List<string>
        {
            // No WriteLog expected for non-DML per driver rule
        };
        
        qp.ExecuteQuery(query, 1);
        observer.Verify("Invalid Query", expectedEvents);
    }
}