namespace mDBMS.Common.Models;

public class DataDeletion
{
    public string Table { get; set; }
    public Condition? Condition { get; set; }

    public DataDeletion(string table, Condition? condition = null)
    {
        Table = table;
        Condition = condition;
    }
}