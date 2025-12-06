using mDBMS.Common.QueryData;
using mDBMS.Common.Interfaces;

namespace mDBMS.QueryProcessorDriver.Mocks;

public class MockQueryOptimizer : IQueryOptimizer
{
    private readonly TestObserver _observer;

    public MockQueryOptimizer(TestObserver observer)
    {
        _observer = observer;
    }

    public Query ParseQuery(string sql)
    {
        _observer.Record($"QueryOptimizer.ParseQuery(SQL='{sql}')");
        // Return a simple query object
        var query = new Query();
        if(sql.Contains("FROM"))
        {
            query.Table = sql.Split("FROM")[1].Trim().Split(" ")[0];
        }
        else if(sql.Contains("INTO"))
        {
            query.Table = sql.Split("INTO")[1].Trim().Split(" ")[0];
        }
        return query;
    }

    public QueryPlan OptimizeQuery(Query query)
    {
        _observer.Record($"QueryOptimizer.OptimizeQuery(Table='{query.Table}')");
        
        // Create a simple plan tree with a TableScanNode as the root
        var planTree = new TableScanNode { TableName = query.Table };
        
        // Return a dummy plan with the created plan tree
        return new QueryPlan { OriginalQuery = query, PlanTree = planTree };
    }

    public double GetCost(QueryPlan plan) { _observer.Record("QueryOptimizer.GetCost"); return 1.0; }
    public IEnumerable<QueryPlan> GenerateAlternativePlans(Query query) { _observer.Record("QueryOptimizer.GenerateAlternativePlans"); return new List<QueryPlan>(); }
}
