namespace mDBMS.Common.Models;

public class Condition
{
    public string Column { get; set; }
    public string Operator { get; set; }
    public object Value { get; set; }

    public Condition(string column, string operatorSymbol, object value)
    {
        Column = column;
        Operator = operatorSymbol;
        Value = value;
    }
}