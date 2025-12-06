using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    private IEnumerable<Row>? cachedRows = null;

    public IEnumerable<Row> VisitSortNode(SortNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Sort");

        if (cachedRows != null) return cachedRows;

        Row[] rows = node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)).ToArray();

        int rowCount = rows.Length;
        if (rowCount < 2) return rows;
        
        Row[] buffer = new Row[rowCount];

        for (int i = 1; i < rowCount; i *= 2)
        {
            for (int j = 0; j < rowCount; j += 2 * i)
            {
                int lidx = j;
                int ridx = Math.Min(j + i, rowCount);
                int lbound = ridx;
                int rbound = Math.Min(j + 2 * i, rowCount);
                int bufferIdx = lidx;

                while (lidx < lbound && ridx < rbound)
                {
                    bool takeLeft = true;
                    
                    foreach (var ordering in node.OrderBy)
                    {
                        var lval = (IComparable) rows[lidx][ordering.Column];
                        var rval = (IComparable) rows[ridx][ordering.Column];
                        int comparison = lval.CompareTo(rval);

                        if (comparison == 0) continue;

                        takeLeft = (comparison < 0) == ordering.IsAscending;
                        break;
                    }
                    
                    if (takeLeft)
                    {
                        buffer[bufferIdx++] = rows[lidx++];
                    }
                    else
                    {
                        buffer[bufferIdx++] = rows[ridx++];
                    }
                }

                while (lidx < lbound)
                {
                    buffer[bufferIdx++] = rows[lidx++];
                }

                while (ridx < rbound)
                {
                    buffer[bufferIdx++] = rows[ridx++];
                }
            }

            for(int k = 0; k < rowCount; k++)
            {
                rows[k] = buffer[k];
            }
        }

        cachedRows = rows;

        return rows;
    }
}
