using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

class MergeSortOperator : Operator
{
    private IEnumerable<Row>? rows;

    public MergeSortOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        this.usePreviousTable = queryPlanStep.Table == "";
    }

    public override IEnumerable<Row> GetRows()
    {
        // TODO: Use materialized table to store rows when number of rows is large
        if (this.rows != null) return this.rows;

        var orderParams = (IEnumerable<Dictionary<string, object?>>) queryPlanStep.Parameters["orderBy"]!;

        this.rows = usePreviousTable ? localTableStorage.lastResult : FetchRows(queryPlanStep.Table);
        Row[] rows = this.rows.ToArray();
        this.rows = rows;
        
        int rowCount = rows.Length;
        int loopBoundary = rowCount * 2;
        Row[] buffer = new Row[rowCount];

        for (int i = 2; i < 2 * loopBoundary; i *= 2)
        {
            for (int j = 0; j < rowCount; j += i)
            {
                int bufferCap = Math.Min(i, rowCount - j);
                int halfsize = bufferCap / 2;
                int bufferSize = 0;
                int lidx = 0;
                int ridx = halfsize;

                while (lidx < halfsize && ridx < bufferCap)
                {
                    var rowEqual = true;
                    foreach (var ordering in orderParams)
                    {
                        var lval = (IComparable) rows[j + lidx][(string) ordering["column"]!];
                        var rval = (IComparable) rows[j + ridx][(string) ordering["column"]!];

                        if (lval == rval) continue;
                        rowEqual = false;

                        var isAscending = (bool) ordering["ascending"]!;

                        if ((lval.CompareTo(rval) < 0) == isAscending)
                        {
                            buffer[bufferSize++] = rows[j + lidx++];
                        }
                        else
                        {
                            buffer[bufferSize++] = rows[j + ridx++];
                        }

                        break;
                    }

                    if (rowEqual)
                    {
                        buffer[bufferSize++] = rows[j + lidx++];
                        buffer[bufferSize++] = rows[j + ridx++];
                    }
                }

                while (lidx < halfsize)
                {
                    buffer[bufferSize++] = rows[j + lidx++];
                }

                while (ridx < bufferSize)
                {
                    buffer[bufferSize++] = rows[j + ridx++];
                }

                for (int l = 0; l < bufferSize; l++)
                {
                    rows[j + l] = buffer[l];
                }
            }
        }

        return rows;
    }

    private IEnumerable<Row> FetchRows(string tablename)
    {
        // TODO: Refactor this to read all blocks when specifying which block to access becomes possible
        Statistic tableStats = storageManager.GetStats(tablename);
        for (int i = 0; i < tableStats.BlockCount; i++)
        {
            IEnumerable<Row> rowsInBlock = storageManager.ReadBlock(new(tablename, []));
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