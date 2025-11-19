using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
using System.Text.RegularExpressions;
using System.Linq;

namespace mDBMS.QueryProcessor.DML;

class HashJoinOperator : Operator
{
    public HashJoinOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        IEnumerable<Row> lhs = localTableStorage.lastResult;
        IEnumerable<Row> rhs;

        if (string.IsNullOrEmpty(queryPlanStep.Table))
        {
            lhs = localTableStorage.holdStorage;
            rhs = localTableStorage.lastResult;
        }
        else
        {
            lhs = localTableStorage.lastResult;
            rhs = FetchRows(queryPlanStep.Table);
        }

        if (lhs == null || rhs == null) yield break;

        var buildList = lhs.ToList();
        if (buildList.Count == 0) yield break;

        string? leftKeyCol = null; // Deklarasikan sebagai nullable
        string? rightKeyCol = null; // Deklarasikan sebagai nullable
        
        // 1. Ambil Key Join dari Parameter "on" (Priority 1)
        if (queryPlanStep.Parameters.TryGetValue("on", out var onObj) && onObj is string onCondition)
        {
            (leftKeyCol, rightKeyCol) = ParseJoinCondition(onCondition);
        }
        else
        {
            // 2. Fallback: Identify Natural Keys (Priority 2)
            var keys = IdentifyNaturalKeys(buildList.First(), rhs);
            if (keys.HasValue) // CEK NULL HARUS DILAKUKAN
            {
                leftKeyCol = keys.Value.Item1;
                rightKeyCol = keys.Value.Item2;
            }
        }

        // Cek jika penentuan key gagal
        if (string.IsNullOrEmpty(leftKeyCol) || string.IsNullOrEmpty(rightKeyCol)) yield break;

        // 3. BUILD PHASE
        var hashTable = new Dictionary<string, List<Row>>();
        foreach (var row in buildList)
        {
            string? keyVal = GetValueByColumnName(row, leftKeyCol!);
            if (keyVal == null) continue;

            if (!hashTable.ContainsKey(keyVal)) hashTable[keyVal] = new List<Row>();
            hashTable[keyVal].Add(row);
        }

        // 4. PROBE PHASE
        foreach (var rightRow in rhs)
        {
            string? keyVal = GetValueByColumnName(rightRow, rightKeyCol!);
            
            if (keyVal != null && hashTable.TryGetValue(keyVal, out var matches))
            {
                foreach (var leftRow in matches) yield return MergeRows(leftRow, rightRow);
            }
        }
    }

    // Helper Methods

    private (string?, string?) ParseJoinCondition(string? condition)
    {
        if (string.IsNullOrEmpty(condition)) return (null, null);
        var parts = condition.Split('=').Select(p => p.Trim()).ToArray();
        return (parts.Length == 2) ? (parts[0], parts[1]) : (null, null);
    }
    
    private (string, string)? IdentifyNaturalKeys(Row leftSample, IEnumerable<Row> rhsEnumerable)
    {
        var rhsEnum = rhsEnumerable.GetEnumerator();
        if (!rhsEnum.MoveNext()) return null;
        var rightSample = rhsEnum.Current;

        foreach (var lKey in leftSample.Columns.Keys)
        {
            foreach (var rKey in rightSample.Columns.Keys)
            {
                string lName = lKey.Split('.').Last();
                string rName = rKey.Split('.').Last();
                if (string.Equals(lName, rName, StringComparison.OrdinalIgnoreCase)) return (lKey, rKey);
            }
        }
        return null;
    }

    private string? GetValueByColumnName(Row row, string colName)
    {
        if (row.Columns.ContainsKey(colName)) return row.Columns[colName]?.ToString();
        var match = row.Columns.Keys.FirstOrDefault(k => k.EndsWith($".{colName}") || colName.EndsWith($".{k}"));
        return (match != null) ? row.Columns[match]?.ToString() : null;
    }

    private Row MergeRows(Row left, Row right)
    {
        var newRow = new Row();
        foreach (var c in left.Columns) newRow.Columns[c.Key] = c.Value;
        foreach (var c in right.Columns) { if (!newRow.Columns.ContainsKey(c.Key)) newRow.Columns[c.Key] = c.Value; }
        return newRow;
    }

    private IEnumerable<Row> FetchRows(string table)
    {
        var rows = storageManager.ReadBlock(new DataRetrieval(table, new[] { "*" }));
        if (rows == null) yield break;

        foreach (var r in rows)
        {
            var norm = new Row();
            foreach (var c in r.Columns)
            {
                string key = c.Key.Contains('.') ? c.Key : $"{table}.{c.Key}";
                norm.Columns[key] = c.Value;
            }
            yield return norm;
        }
    }
}