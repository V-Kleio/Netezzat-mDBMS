namespace mDBMS.Common.Models;

public class DataRetrieval
{
    public string Table { get; set; }
    public string[] Columns { get; set; }
    public Condition? Condition { get; set; }

    public DataRetrieval(string table, string[] columns, Condition? condition = null)
    {
        Table = table;
        Columns = columns;
        Condition = condition;
    }
}
