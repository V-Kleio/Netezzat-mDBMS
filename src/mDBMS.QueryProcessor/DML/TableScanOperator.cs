using System;
using System.Collections.Generic;
using System.Linq;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

/// <summary>
/// operator table scan dasar dengan dukungan kolom dan kondisi dari QueryPlanStep.
/// </summary>
class TableScanOperator : Operator
{
    private readonly string _tableName;
    private readonly string[] _columns;
    private readonly Condition? _condition;

    public TableScanOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        usePreviousTable = false;
        _tableName = ResolveTableName(queryPlanStep);
        _columns = ResolveColumns(queryPlanStep, _tableName);
        _condition = ResolveCondition(queryPlanStep);
    }

    public override IEnumerable<Row> GetRows()
    {
        if (string.IsNullOrWhiteSpace(_tableName))
        {
            yield break;
        }

        var retrieval = new DataRetrieval(_tableName, _columns, [[_condition]]);
        var rows = storageManager.ReadBlock(retrieval) ?? Enumerable.Empty<Row>();

        foreach (var row in rows)
        {
            yield return PrefixColumns(row, _tableName);
        }
    }

    private static string ResolveTableName(QueryPlanStep step)
    {
        if (!string.IsNullOrWhiteSpace(step.Table))
        {
            return step.Table;
        }

        if (step.Parameters.TryGetValue("table", out var tableObj) && tableObj is string table && !string.IsNullOrWhiteSpace(table))
        {
            return table;
        }

        return string.Empty;
    }

    private static string[] ResolveColumns(QueryPlanStep step, string tableName)
    {
        if (step.Parameters.TryGetValue("columns", out var value))
        {
            if (value is IEnumerable<string> enumerable)
            {
                var normalized = enumerable
                    .Where(column => !string.IsNullOrWhiteSpace(column))
                    .Select(column => QualifyColumn(column, tableName))
                    .ToArray();

                if (normalized.Length > 0)
                {
                    return normalized;
                }
            }
            else if (value is string single && !string.IsNullOrWhiteSpace(single))
            {
                return new[] { QualifyColumn(single, tableName) };
            }
        }

        return new[] { "*" };
    }

    private static Condition? ResolveCondition(QueryPlanStep step)
    {
        if (step.Parameters.TryGetValue("condition", out var conditionObj) && conditionObj is Condition condition)
        {
            return condition;
        }

        if (step.Parameters.TryGetValue("predicate", out var predicateObj) && predicateObj is Condition predicate)
        {
            return predicate;
        }

        return null;
    }

    private static string QualifyColumn(string column, string tableName)
    {
        if (string.IsNullOrWhiteSpace(column) || column == "*")
        {
            return column;
        }

        return column.Contains('.') ? column : $"{tableName}.{column}";
    }

    private static Row PrefixColumns(Row sourceRow, string tableName)
    {
        var prefixedRow = new Row();

        foreach (var kvp in sourceRow.Columns)
        {
            var prefixedName = kvp.Key.Contains('.') ? kvp.Key : $"{tableName}.{kvp.Key}";
            prefixedRow.Columns[prefixedName] = kvp.Value;
        }

        return prefixedRow;
    }
}
