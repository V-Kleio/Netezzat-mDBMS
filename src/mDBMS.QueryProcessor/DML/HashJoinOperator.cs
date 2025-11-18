using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class HashJoinOperator : Operator
{
    public HashJoinOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        // 1. Siapkan Input Kiri (Build) dan Kanan (Probe)
        IEnumerable<Row> lhs = localTableStorage.lastResult;
        IEnumerable<Row> rhs;

        if (string.IsNullOrEmpty(queryPlanStep.Table))
        {
            lhs = localTableStorage.holdStorage; // Input 1 (Pipeline)
            rhs = localTableStorage.lastResult;  // Input 2 (Pipeline)
        }
        else
        {
            lhs = localTableStorage.lastResult;  // Input 1 (Pipeline)
            rhs = FetchRows(queryPlanStep.Table); // Input 2 (Disk)
        }

        if (lhs == null || rhs == null) yield break;

        // 2. Materialize Build Side (Kiri) ke Memory
        var buildList = lhs.ToList();
        if (buildList.Count == 0) yield break;

        // 3. Deteksi Key Join (Natural Join berdasarkan suffix nama kolom yang sama)
        var sampleLeft = buildList.First();
        var joinPairs = new List<(string lKey, string rKey)>();

        // Kita perlu intip satu baris kanan untuk mencocokkan kolom
        var rhsEnumerator = rhs.GetEnumerator();
        if (!rhsEnumerator.MoveNext()) yield break; // Kanan kosong
        var sampleRight = rhsEnumerator.Current;

        foreach (var lKey in sampleLeft.Columns.Keys)
        {
            foreach (var rKey in sampleRight.Columns.Keys)
            {
                // Bandingkan hanya nama kolom (abaikan nama tabel)
                string lName = lKey.Contains('.') ? lKey.Split('.').Last() : lKey;
                string rName = rKey.Contains('.') ? rKey.Split('.').Last() : rKey;

                if (string.Equals(lName, rName, StringComparison.OrdinalIgnoreCase))
                {
                    joinPairs.Add((lKey, rKey));
                }
            }
        }

        if (joinPairs.Count == 0) yield break;

        // 4. Build phase : buat Hash Table dari Kiri
        var hashTable = new Dictionary<string, List<Row>>();
        foreach (var row in buildList)
        {
            string key = GenerateKey(row, joinPairs.Select(p => p.lKey));
            if (!hashTable.ContainsKey(key)) hashTable[key] = new List<Row>();
            hashTable[key].Add(row);
        }

        // 5. Probe phase : proses kanan (mulai dari yang sudah di intip)
        do 
        {
            var rightRow = rhsEnumerator.Current;
            string key = GenerateKey(rightRow, joinPairs.Select(p => p.rKey));

            if (hashTable.TryGetValue(key, out var matches))
            {
                foreach (var leftRow in matches)
                {
                    yield return MergeRows(leftRow, rightRow);
                }
            }
        } while (rhsEnumerator.MoveNext());
    }

    // Helper Ringkas
    private string GenerateKey(Row row, IEnumerable<string> cols)
    {
        return string.Join("|", cols.Select(c => row.Columns.ContainsKey(c) ? row.Columns[c]?.ToString() : "NULL"));
    }

    private Row MergeRows(Row left, Row right)
    {
        var newRow = new Row();
        foreach (var c in left.Columns) newRow.Columns[c.Key] = c.Value;
        foreach (var c in right.Columns) newRow.Columns[c.Key] = c.Value; // Timpa jika duplikat
        return newRow;
    }

    private IEnumerable<Row> FetchRows(string table)
    {
        // Baca dari disk & Normalisasi "Kolom" jadi "Tabel.Kolom"
        var rows = storageManager.ReadBlock(new DataRetrieval(table, new[] { "*" }));
        if (rows == null) yield break;

        foreach (var r in rows)
        {
            var normRow = new Row();
            foreach (var c in r.Columns)
            {
                string key = c.Key.Contains('.') ? c.Key : $"{table}.{c.Key}";
                normRow.Columns[key] = c.Value;
            }
            yield return normRow;
        }
    }
}