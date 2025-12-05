using System.Runtime.CompilerServices;
using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> MergeJoin(JoinNode node)
    {
        string ljoin = (string) node.JoinCondition.lhs;
        string rjoin = (string) node.JoinCondition.rhs;

        Dictionary<string, Type> leftKeys = [];
        Dictionary<string, Type> rightKeys = [];

        SortNode leftsort = new(node.Left, [new() { Column = ljoin }]);
        SortNode rightsort = new(node.Right, [new() { Column = rjoin }]);

        IEnumerable<Row> lhs = leftsort.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));
        IEnumerable<Row> rhs = rightsort.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));

        IEnumerator<Row> litr = lhs.GetEnumerator();
        IEnumerator<Row> ritr = rhs.GetEnumerator();

        object? currentMatcher = null;
        List<Row> leftMatchers = [];
        List<Row> rightMatchers = [];

        while (litr.MoveNext() && ritr.MoveNext())
        {
            Row leftRow = litr.Current;
            Row rightRow = ritr.Current;

            if (leftKeys.Count == 0)
            {
                foreach (var (key, val) in leftRow.Columns)
                {
                    leftKeys[key] = val.GetType();
                }
            }

            if (rightKeys.Count == 0)
            {
                foreach (var (key, val) in rightRow.Columns)
                {
                    rightKeys[key] = val.GetType();
                }
            }

            if (leftRow[ljoin] == currentMatcher)
            {
                litr.MoveNext();
            }
            else if (rightRow[rjoin] == currentMatcher)
            {
                ritr.MoveNext();
            }
            else
            {
                int order = ((IComparable) leftRow.Columns[ljoin]).CompareTo(rightRow.Columns[rjoin]);
                bool popLeft = order < 0;

                if (rightMatchers.Count == 0)
                {
                    foreach (Row rightMatch in rightMatchers)
                    {
                        Row row = new();
                    
                        foreach (var (key, val) in rightMatch.Columns)
                        {
                            row[key] = val;
                        }
        
                        foreach (var (key, val) in leftKeys)
                        {
                            row[key] = RuntimeHelpers.GetUninitializedObject(val);
                        }
        
                        row.id = rightMatch.id;
        
                        yield return row;
                    }
                }
                else if (leftMatchers.Count == 0)
                {
                    foreach (Row leftMatch in leftMatchers)
                    {
                        Row row = new();
                    
                        foreach (var (key, val) in leftMatch.Columns)
                        {
                            row[key] = val;
                        }
        
                        foreach (var (key, val) in rightKeys)
                        {
                            row[key] = RuntimeHelpers.GetUninitializedObject(val);
                        }
        
                        row.id = leftMatch.id;
        
                        yield return row;
                    }
                }
                else
                {
                    foreach (Row leftMatch in leftMatchers)
                    {
                        foreach (Row rightMatch in rightMatchers)
                        {
                            Row row = new();
                        
                            foreach (var (key, val) in leftMatch.Columns)
                            {
                                row[key] = val;
                            }
            
                            foreach (var (key, val) in rightMatch.Columns)
                            {
                                row[key] = val;
                            }
            
                            row.id = leftMatch.id + ";" + rightMatch.id;
            
                            yield return row;
                        }
                    }
                }

                currentMatcher = popLeft ? leftRow[ljoin] : rightRow[rjoin];
            }
        }
    }
}