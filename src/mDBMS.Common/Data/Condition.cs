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
}