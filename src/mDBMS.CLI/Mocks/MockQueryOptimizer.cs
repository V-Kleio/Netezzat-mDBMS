using System;
using System.Collections.Generic;
using System.Linq;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;

namespace mDBMS.CLI.Mocks
{
    public class MockQueryOptimizer : IQueryOptimizer
    {
        public ISelection ParseQuery(string query)
        {
            Console.WriteLine($"[MOCK QO]: ParseQuery dipanggil dengan '{query}'.");
            return new TableNode(query);
        }

        public ISelection OptimizeQuery(ISelection query, IEnumerable<Statistic> statistics)
        {
            Console.WriteLine($"[MOCK QO]: OptimizeQuery dipanggil. Statistik diterima: {statistics.Count()}.");
            return query;
        }
    }
}
