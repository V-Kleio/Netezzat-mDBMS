using System.Runtime.CompilerServices;
using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> MergeJoin(JoinNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Merge Join antara 2 tabel");
        if (node.JoinCondition == null)
        {
            throw new Exception("MergeJoin requires a join condition. Use CrossJoin for Cartesian product.");
        }

        string ljoin = (string) node.JoinCondition.lhs;
        string rjoin = (string) node.JoinCondition.rhs;

        Dictionary<string, Type>? leftKeys = null;
        Dictionary<string, Type>? rightKeys = null;

        SortNode leftsort = new(node.Left, [new() { Column = ljoin }]);
        SortNode rightsort = new(node.Right, [new() { Column = rjoin }]);

        List<Row> leftRows = leftsort.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)).ToList();
        List<Row> rightRows = rightsort.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)).ToList();

        bool[] leftMatched = new bool[leftRows.Count];
        bool[] rightMatched = new bool[rightRows.Count];

        if (leftRows.Count > 0)
        {
            leftKeys = leftRows[0].Columns.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.GetType());
        }
        if (rightRows.Count > 0)
        {
            rightKeys = rightRows[0].Columns.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.GetType());
        }

        int leftIdx = 0;
        int rightIdx = 0;

        while (leftIdx < leftRows.Count && rightIdx < rightRows.Count)
        {
            Row leftRow = leftRows[leftIdx];
            Row rightRow = rightRows[rightIdx];

            var leftVal = (IComparable) leftRow[ljoin];
            var rightVal = rightRow[rjoin];

            int cmp = leftVal.CompareTo(rightVal);

            if (cmp < 0)
            {
                leftIdx++;
            }
            else if (cmp > 0)
            {
                rightIdx++;
            }
            else
            {
                int leftStart = leftIdx;
                int rightStart = rightIdx;

                while (leftIdx < leftRows.Count && Equals(leftRows[leftIdx][ljoin], leftVal))
                {
                    leftIdx++;
                }

                while (rightIdx < rightRows.Count && Equals(rightRows[rightIdx][rjoin], rightVal))
                {
                    rightIdx++;
                }

                for (int l = leftStart; l < leftIdx; l++)
                {
                    for (int r = rightStart; r < rightIdx; r++)
                    {
                        leftMatched[l] = true;
                        rightMatched[r] = true;

                        Row row = new();

                        foreach (var (key, val) in leftRows[l].Columns)
                        {
                            row[key] = val;
                        }

                        foreach (var (key, val) in rightRows[r].Columns)
                        {
                            row[key] = val;
                        }

                        row.id = leftRows[l].id + ";" + rightRows[r].id;

                        yield return row;
                    }
                }
            }
        }

        if (node.JoinType == JoinType.LEFT || node.JoinType == JoinType.FULL)
        {
            for (int i = 0; i < leftRows.Count; i++)
            {
                if (!leftMatched[i] && rightKeys != null)
                {
                    Row row = new() { id = leftRows[i].id };

                    foreach (var (key, val) in leftRows[i].Columns)
                    {
                        row[key] = val;
                    }

                    foreach (var (key, val) in rightKeys)
                    {
                        row[key] = RuntimeHelpers.GetUninitializedObject(val);
                    }

                    yield return row;
                }
            }
        }

        if (node.JoinType == JoinType.RIGHT || node.JoinType == JoinType.FULL)
        {
            for (int i = 0; i < rightRows.Count; i++)
            {
                if (!rightMatched[i] && leftKeys != null)
                {
                    Row row = new() { id = rightRows[i].id };

                    foreach (var (key, val) in leftKeys)
                    {
                        row[key] = RuntimeHelpers.GetUninitializedObject(val);
                    }

                    foreach (var (key, val) in rightRows[i].Columns)
                    {
                        row[key] = val;
                    }

                    yield return row;
                }
            }
        }
    }
}
