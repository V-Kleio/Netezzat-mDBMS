using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;

namespace mDBMS.QueryProcessor.DML;

class ProjectionOperator : Operator
{
    public ProjectionOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        var inputRows = localTableStorage.lastResult;
        if (inputRows == null || !inputRows.Any())
        {
            yield break;
        }

        // 1. Parse kolom target dari deskripsi query plan step
        var rawTargetColumns = ParseColumnsFromDescription(queryPlanStep.Description);
        bool selectAll = rawTargetColumns.Contains("*");

        // 2. Normalisasi nama kolom dengan menambahkan prefix tabel jika perlu
        var targetColumns = new List<string>();
        if (!selectAll)
        {
            string currentTable = queryPlanStep.Table; 
            foreach (var col in rawTargetColumns)
            {
                if (!col.Contains('.') && !string.IsNullOrEmpty(currentTable))
                {
                    targetColumns.Add($"{currentTable}.{col}");
                }
                else
                {
                    targetColumns.Add(col);
                }
            }
        }

        // 3. Proses Proyeksi
        foreach (var row in inputRows)
        {
            var projectedRow = new Row();

            if (selectAll)
            {
                foreach (var col in row.Columns)
                {
                    projectedRow.Columns[col.Key] = col.Value;
                }
            }
            else
            {
                foreach (var colName in targetColumns)
                {
                    if (row.Columns.ContainsKey(colName))
                    {
                        projectedRow.Columns[colName] = row.Columns[colName];
                    }
                    else 
                    {
                         var shortMatch = row.Columns.Keys.FirstOrDefault(k => k.EndsWith($".{colName}") || k == colName);
                         if (shortMatch != null)
                         {
                             projectedRow.Columns[colName] = row.Columns[shortMatch];
                         }
                    }
                }
            }

            yield return projectedRow;
        }
    }

    private List<string> ParseColumnsFromDescription(string description)
    {
        string prefix = "Project columns: ";
        
        if (!string.IsNullOrEmpty(description) && description.StartsWith(prefix))
        {
            var columnsPart = description.Replace(prefix, "").Trim();
            return columnsPart.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
        }

        return new List<string> { "*" };
    }
}