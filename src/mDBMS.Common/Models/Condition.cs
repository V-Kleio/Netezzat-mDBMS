namespace mDBMS.Common.Models;

public class Condition
{
    public string lhs = "";
    public string rhs = "";
    public Operation opr;

    public enum Operation
    {
        EQ,
        NEQ,
        GT,
        GEQ,
        LT,
        LEQ
    }
}