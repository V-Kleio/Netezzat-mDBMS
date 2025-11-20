using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

class MergeJoinOperator : Operator
{
    private IEnumerable<Row>? lhs;
    private IEnumerable<Row>? rhs;
    private List<(string, string)> joinColumns = [];

    public MergeJoinOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        this.usePreviousTable = queryPlanStep.Table == "";
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

            setJoinColumns((string) queryPlanStep.Parameters["on"]!);

            // TODO: Detect if the inputs are already sorted before sorting
            LocalTableStorage lhsSortStorage = new() { lastResult = lhs };
            LocalTableStorage rhsSortStorage = new() { lastResult = rhs };
            
            QueryPlanStep lhsSortStep = new()
            {
                Parameters = new()
                {
                    ["orderBy"] = joinColumns.Select(
                        (tuple) => new Dictionary<string, object?>()
                        {
                            ["column"] = tuple.Item1,
                            ["ascending"] = true
                        }
                    )
                }
            };
    
            QueryPlanStep rhsSortStep = new()
            {
                Parameters = new()
                {
                    ["orderBy"] = joinColumns.Select(
                        (tuple) => new Dictionary<string, object?>()
                        {
                            ["column"] = tuple.Item2,
                            ["ascending"] = true
                        }
                    )
                }
            };

            lhs = new MergeSortOperator(storageManager, lhsSortStep, lhsSortStorage).GetRows();
            rhs = new MergeSortOperator(storageManager, lhsSortStep, lhsSortStorage).GetRows();
        }

        IEnumerator<Row> liter = lhs.GetEnumerator();
        IEnumerator<Row> riter = rhs.GetEnumerator();

        while (liter.MoveNext() && riter.MoveNext())
        {
            Row leftRow = liter.Current;
            Row rightRow = riter.Current;

            bool matches = true;
            bool popLeft = true;

            foreach((string col1, string col2) in joinColumns)
            {
                int order = ((IComparable) leftRow.Columns[col1]).CompareTo(rightRow.Columns[col2]);

                if (order == 0) continue;

                matches = false;
                popLeft = order < 0;
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

                liter.MoveNext();
                riter.MoveNext();
            }
            else if (popLeft)
            {
                liter.MoveNext();
            }
            else
            {
                riter.MoveNext();
            }
        }
    }

    private void setJoinColumns(string onString)
    {
        if (onString.Trim() != "")
        {
            List<(string, string)> joinColumns = [];
    
            foreach (string operation in onString.Split(',').Select(s => s.Trim()))
            {
                var operands = operation.Split('=').Select(s => s.Trim()).ToArray();
    
                if (operands.Length != 2)
                {
                    throw new Exception("join on operands");
                }
    
                joinColumns.Add((operands[0], operands[1]));
            }
    
            this.joinColumns = joinColumns;
        }
        else
        {
            Row leftSample = lhs!.FirstOrDefault(new Row());
            Row rightSample = rhs!.FirstOrDefault(new Row());

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