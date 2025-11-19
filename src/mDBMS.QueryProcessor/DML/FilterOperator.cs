using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class FilterOperator : Operator
{
    public FilterOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        var input = localTableStorage.lastResult;
        if (input == null)
        {
            yield break;
        }

        // Parse Filter Expression
        string expr = queryPlanStep.Description.Replace("Filter:", "").Trim();

        if (string.IsNullOrWhiteSpace(expr))
        {
            yield break;
        }

        // Parse Operator
        string[] ops = new[] {">=", "<=", "<>", "!=", "=", ">", "<"};
        string usedOp = ops.FirstOrDefault(op => expr.Contains(op)) ?? throw new Exception("Operator filter tidak ditemukan.");

        var parts = expr.Split(usedOp, 2);
        string lhs = parts[0].Trim();
        string rhsRaw = parts[1].Trim();

        string rhs = rhsRaw.Trim('\'', '"');

        // Search lhs column
        string? lhsColumn = FindMatchingColumn(input, lhs);
        if (lhsColumn == null)
        {
            yield break;
        }

        bool rhsIsColumn =
            !double.TryParse(rhs, out _) &&
            !rhsRaw.StartsWith("'") &&
            !rhsRaw.StartsWith("\"") &&
            rhsRaw.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '.');

        string? rhsColumn = null;
        if (rhsIsColumn)
        {
            rhsColumn = FindMatchingColumn(input, rhs);
            if (rhsColumn == null)
            {
                yield break;
            }
        }

        foreach (var row in input)
        {
            if (row == null)
            {
                continue;
            }

            object lhsValue = row.Columns[lhsColumn];

            object rhsValue = rhsIsColumn ? row.Columns[rhsColumn!] : (object)rhs;

            if (RowPassesFilter(lhsValue, rhsValue, usedOp))
            {
                yield return row;
            }
        }
    }

    private string? FindMatchingColumn(IEnumerable<Row> rows, string target)
    {
        foreach (var row in rows)
        {
            if (row == null) continue;

            foreach (var col in row.Columns.Keys)
            {
                if (col.Equals(target, StringComparison.OrdinalIgnoreCase))
                    return col;

                if (col.EndsWith("." + target, StringComparison.OrdinalIgnoreCase))
                    return col;
            }
        }
        return null;
    } 

    private bool RowPassesFilter(object lhs, object rhs, string op)
    {
        // Numeric comparison
        bool lhsIsNum = double.TryParse(lhs?.ToString(), out double lnum);
        bool rhsIsNum = double.TryParse(rhs?.ToString(), out double rnum);

        if (lhsIsNum && rhsIsNum)
        {
            return op switch
            {
                "=" => lnum == rnum,
                "!=" or "<>" => lnum != rnum,
                ">" => lnum > rnum,
                "<" => lnum < rnum,
                ">=" => lnum >= rnum,
                "<=" => lnum <= rnum,
                _ => false
            };
        }
        else
        {
            // String comparison
            string ls = lhs?.ToString() ?? "";
            string rs = rhs?.ToString() ?? "";

            return op switch
            {
                "=" => ls.Equals(rs, StringComparison.OrdinalIgnoreCase),
                "!=" or "<>" => !ls.Equals(rs, StringComparison.OrdinalIgnoreCase),
                _ => false
            };
        }
    }
}