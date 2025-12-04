using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    private IEnumerable<Row>? cachedRows = null;

    public IEnumerable<Row> VisitSortNode(SortNode node)
    {
        if (cachedRows != null) return cachedRows;

        Row[] rows = node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)).ToArray();

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
                    foreach (var ordering in node.OrderBy)
                    {
                        var lval = (IComparable) rows[j + lidx][ordering.Column];
                        var rval = (IComparable) rows[j + ridx][ordering.Column];

                        if (lval == rval) continue;
                        rowEqual = false;

                        var isAscending = ordering.IsAscending;

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

        cachedRows = rows;

        return rows;
    }
}