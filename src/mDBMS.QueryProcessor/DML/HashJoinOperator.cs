using System.Runtime.CompilerServices;
using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> HashJoin(JoinNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Hash Join antara 2 tabel");

        string ljoin = (string) node.JoinCondition.lhs;
        string rjoin = (string) node.JoinCondition.rhs;

        Dictionary<string, Type> leftKeys = [];
        Dictionary<string, Type> rightKeys = [];

        IEnumerable<Row> lhs = node.Left.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));
        IEnumerable<Row> rhs = node.Right.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId));

        Dictionary<object, HashJoinBucket> hashtable = [];

        foreach (Row leftRow in lhs)
        {
            if (leftKeys.Count == 0)
            {
                foreach (var (key, val) in leftRow.Columns)
                {
                    leftKeys[key] = val.GetType();
                }
            }

            if (!hashtable.ContainsKey(leftRow[ljoin]))
            {
                hashtable[leftRow[ljoin]] = new();
            }

            hashtable[leftRow[ljoin]].Matchers.Add(leftRow);
        }

        foreach (Row rightRow in rhs)
        {
            if (rightKeys.Count == 0)
            {
                foreach (var (key, rightVal) in rightRow.Columns)
                {
                    rightKeys[key] = rightVal.GetType();
                }
            }

            if (hashtable.TryGetValue(rightRow[rjoin], out var bucket))
            {
                bucket.Matches++;

                foreach (Row leftRow in bucket.Matchers)
                {
                    Row row = new();
                        
                    foreach (var (key, val) in leftRow.Columns)
                    {
                        row[key] = val;
                    }
    
                    foreach (var (key, val) in rightRow.Columns)
                    {
                        row[key] = val;
                    }
    
                    row.id = leftRow.id + ";" + rightRow.id;
    
                    yield return row;
                }
            }
            else if (node.JoinType == JoinType.RIGHT || node.JoinType == JoinType.FULL)
            {
                Row row = new();
                        
                foreach (var (key, val) in leftKeys)
                {
                    row[key] = RuntimeHelpers.GetUninitializedObject(val);
                }

                foreach (var (key, val) in rightRow.Columns)
                {
                    row[key] = val;
                }

                row.id = rightRow.id;

                yield return row;
            }
        }

        if (node.JoinType == JoinType.LEFT || node.JoinType == JoinType.FULL)
        {
            foreach (var (_, bucket) in hashtable)
            {
                if (bucket.Matches == 0)
                {
                    foreach (Row leftRow in bucket.Matchers)
                    {
                        Row row = new();
                        
                        foreach (var (key, val) in leftRow.Columns)
                        {
                            row[key] = val;
                        }
        
                        foreach (var (key, val) in rightKeys)
                        {
                            row[key] = RuntimeHelpers.GetUninitializedObject(val);
                        }
        
                        row.id = leftRow.id;
        
                        yield return row;
                    }
                }
            }
        }
    }
}

internal class HashJoinBucket
{
    public int Matches = 0;
    public List<Row> Matchers = [];
}