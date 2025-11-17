using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class NestedLoopJoinOperator : Operator
{
    private IEnumerable<Row>? lhs;
    private IEnumerable<Row>? rhs;
    private HashSet<(string, string)> joinColumns = [];

    public NestedLoopJoinOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        if (lhs == null || rhs == null)
        {
            if (queryPlanStep.Table == "")
            {
                lhs = localTableStorage.holdStorage;
                rhs = localTableStorage.lastResult;
            }
            else
            {
                lhs = localTableStorage.lastResult;
                // TODO: Figure out how to fetch all columns
                rhs = FetchRows(queryPlanStep.Table, []);
            }

            // TODO: Once available, use the join parameters given by the query plan step
            Row leftSample = lhs.FirstOrDefault(new Row());
            Row rightSample = rhs.FirstOrDefault(new Row());

            foreach (string leftAttribute in leftSample.Columns.Keys)
            {
                foreach (string rightAttribute in rightSample.Columns.Keys)
                {
                    if (leftAttribute.Split('.').Last() == rightAttribute.Split('.').Last())
                    {
                        joinColumns.Add((leftAttribute, rightAttribute));
                    }
                }
            }
        }

        foreach (Row leftRow in lhs)
        {
            foreach (Row rightRow in rhs)
            {
                bool matches = true;

                foreach((string col1, string col2) in joinColumns)
                {
                    if (leftRow.Columns[col1] != rightRow.Columns[col2])
                    {
                        matches = false;
                        break;
                    }
                }

                if (matches)
                {
                    Row joinedRow = new Row();

                    foreach (var attribute in leftRow.Columns)
                    {
                        joinedRow[attribute.Key] = attribute.Value;
                    }

                    foreach (var attribute in rightRow.Columns)
                    {
                        joinedRow[attribute.Key] = attribute.Value;
                    }
                    
                    yield return joinedRow;
                }
            }
        }
    }

    private IEnumerable<Row> FetchRows(string tablename, string[] columns)
    {
        // TODO: Refactor this to read all blocks when specifying which block to access becomes possible
        Statistic tableStats = storageManager.GetStats(tablename);
        for (int i = 0; i < tableStats.BlockCount; i++)
        {
            IEnumerable<Row> rowsInBlock = storageManager.ReadBlock(new(tablename, columns));
            foreach (Row rawRow in rowsInBlock)
            {
                Row row = new();
                foreach (var attribute in rawRow.Columns)
                {
                    row[tablename + "." + attribute.Key] = attribute.Value;
                }

                yield return row;
            }
        }
    }
}