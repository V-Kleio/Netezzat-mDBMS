using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class FilterOperator : Operator
{
    private Condition? _condition;

    public FilterOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Inisialisasi state (Usahakan semua state dimuat dalam GetRows)
        this.usePreviousTable = true;

        // Extract condition from parameters
        if (queryPlanStep.Parameters.TryGetValue("Condition", out var ConditionObj) && ConditionObj is Condition condition)
        {
            _condition = condition;
        }
        else
        {
            // Parse from description jika tidak ada di parameters
            _condition = ParseConditionFromDescription(queryPlanStep.Description);
        }
    }

    public override IEnumerable<Row> GetRows()
    {
        var input = localTableStorage.lastResult;
        if (input == null || _condition == null)
        {
            yield break;
        }

        // Search lhs column
        string? lhsColumn = FindMatchingColumn(input, (string) _condition.lhs);
        if (lhsColumn == null)
        {
            yield break;
        }

        // Determine if rhs is column or literal value
        string? rhsColumn = FindMatchingColumn(input, (string) _condition.rhs);
        bool rhsIsColumn = rhsColumn != null;

        // Filter rows
        foreach (var row in input)
        {
            if (row == null)
            {
                continue;
            }

            object lhsValue = row.Columns[lhsColumn];

            object rhsValue = rhsIsColumn ? row.Columns[rhsColumn!] : (object)_condition.rhs;

            if (EvaluateCondition(lhsValue, rhsValue, _condition.opr))
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

    private bool EvaluateCondition(object lhs, object rhs, Condition.Operation operation)
    {
        // Numeric comparison
        bool lhsIsNum = double.TryParse(lhs?.ToString(), out double lnum);
        bool rhsIsNum = double.TryParse(rhs?.ToString(), out double rnum);

        if (lhsIsNum && rhsIsNum)
        {
            return operation switch
            {
                Condition.Operation.EQ => lnum == rnum,
                Condition.Operation.NEQ => lnum != rnum,
                Condition.Operation.GT => lnum > rnum,
                Condition.Operation.LT => lnum < rnum,
                Condition.Operation.GEQ => lnum >= rnum,
                Condition.Operation.LEQ => lnum <= rnum,
                _ => false
            };
        }
        else
        {
            // String comparison
            string ls = lhs?.ToString() ?? "";
            string rs = rhs?.ToString() ?? "";

            return operation switch
            {
                Condition.Operation.EQ => ls.Equals(rs, StringComparison.OrdinalIgnoreCase),
                Condition.Operation.NEQ => !ls.Equals(rs, StringComparison.OrdinalIgnoreCase),
                _ => false
            };
        }
    }

    private Condition? ParseConditionFromDescription(string description)
    {
        string expr = description.Replace("Filter:", "").Trim();
        
        if (string.IsNullOrWhiteSpace(expr))
        {
            return null;
        }

        // Parse operator
        string[] ops = new[] {">=", "<=", "<>", "!=", "=", ">", "<"};
        string? usedOp = ops.FirstOrDefault(op => expr.Contains(op));
        
        if (usedOp == null)
        {
            return null;
        }

        var parts = expr.Split(usedOp, 2);
        if (parts.Length != 2)
        {
            return null;
        }

        string lhs = parts[0].Trim();
        string rhsRaw = parts[1].Trim();
        string rhs = rhsRaw.Trim('\'', '"');

        var operation = usedOp switch
        {
            "=" => Condition.Operation.EQ,
            "!=" or "<>" => Condition.Operation.NEQ,
            ">" => Condition.Operation.GT,
            "<" => Condition.Operation.LT,
            ">=" => Condition.Operation.GEQ,
            "<=" => Condition.Operation.LEQ,
            _ => Condition.Operation.EQ
        };

        return new Condition
        {
            lhs = lhs,
            rhs = rhs,
            opr = operation
        };
    }
}