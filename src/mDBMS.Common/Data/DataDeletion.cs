namespace mDBMS.Common.Data;

public class DataDeletion
{
    public string Table { get; set; }
    public IEnumerable<IEnumerable<Condition>>? Condition { get; set; }

    public DataDeletion(string table, IEnumerable<IEnumerable<Condition>>? condition = null)
    {
        Table = table;
        Condition = condition;
    }
}
