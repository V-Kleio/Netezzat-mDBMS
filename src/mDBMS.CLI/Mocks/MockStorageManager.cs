using System.Collections.Generic;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;

namespace mDBMS.CLI.Mocks
{
    public class MockStorageManager : IStorageManager
    {
        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            // placeholder data
            var rows = new List<Row>
            {
                new Row { Columns = { ["id"] = 1, ["name"] = "Alice" } },
                new Row { Columns = { ["id"] = 2, ["name"] = "Bob" } },
                new Row { Columns = { ["id"] = 3, ["name"] = "Charlie" } }
            };
            return rows;
        }

        public int WriteBlock(DataWrite dataWrite) => 1;

        public int DeleteBlock(DataDeletion dataDeletion) => 1;

        public void SetIndex(string table, string column, IndexType type) { }

        public Statistic GetStats(string tableName)
        {
            return new Statistic
            {
                Table = tableName,
                TupleCount = 3,
                BlockCount = 1,
                TupleSize = 32,
                BlockingFactor = 3,
                DistinctValues = 3
            };
        }
    }
}
