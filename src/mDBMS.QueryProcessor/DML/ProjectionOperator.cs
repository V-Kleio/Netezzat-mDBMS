using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
using System.Linq;

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
        if (inputRows == null || !inputRows.Any()) yield break;

        List<string> targetColumns;
        if (queryPlanStep.Parameters.TryGetValue("columns", out var colsObj) && colsObj is List<string> colsFromParam)
            targetColumns = colsFromParam;
        else
            targetColumns = ParseColumnsFromDescription(queryPlanStep.Description);

        bool selectAll = targetColumns.Contains("*");
        string currentTable = queryPlanStep.Table;

        if (!selectAll)
        {
            for (int i = 0; i < targetColumns.Count; i++)
            {
                if (!targetColumns[i].Contains('.') && !string.IsNullOrEmpty(currentTable))
                    targetColumns[i] = $"{currentTable}.{targetColumns[i]}";
            }
        }

        foreach (var row in inputRows)
        {
            var projectedRow = new Row();

            if (selectAll)
            {
                foreach (var col in row.Columns) projectedRow.Columns[col.Key] = col.Value;
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
                        if (shortMatch != null) projectedRow.Columns[colName] = row.Columns[shortMatch];
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
            return description.Replace(prefix, "").Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
        }
        return new List<string> { "*" };
    }
}