namespace mDBMS.Common.Data;

public class Condition
{
    public object lhs = "";
    public object rhs = "";
    public Operation opr;
    public Relation rel;

    public enum Operation
    {
        EQ,
        NEQ,
        GT,
        GEQ,
        LT,
        LEQ
    }

    public enum Relation
    {
        COLUMN_AND_VALUE,
        VALUE_AND_COLUMN,
        COLUMN_AND_COLUMN,
    }

    /// <summary>
    /// Mengonversi kondisi ke string.
    /// </summary>
    public override string ToString()
    {
        string operatorStr = opr switch
        {
            Operation.EQ => "=",
            Operation.NEQ => "<>",
            Operation.GT => ">",
            Operation.GEQ => ">=",
            Operation.LT => "<",
            Operation.LEQ => "<=",
            _ => "?"
        };

        // Format lhs dan rhs sesuai tipe
        string lhsStr = FormatOperand(lhs);
        string rhsStr = FormatOperand(rhs);

        return $"{lhsStr} {operatorStr} {rhsStr}";
    }

    /// <summary>
    /// Format operand untuk ditampilkan.
    /// String akan dibungkus dengan quotes, nilai lain ditampilkan apa adanya.
    /// </summary>
    private static string FormatOperand(object? operand)
    {
        if (operand == null) return "NULL";
        if (operand is string s)
        {
            // Jika seperti nama kolom (tidak mengandung spasi dan tidak dimulai angka), tampilkan langsung
            // Jika seperti string literal, bungkus dengan quotes
            if (!string.IsNullOrEmpty(s) && !s.Contains(' ') && !char.IsDigit(s[0]))
                return s;
            return $"'{s}'";
        }
        return operand.ToString() ?? "NULL";
    }
}