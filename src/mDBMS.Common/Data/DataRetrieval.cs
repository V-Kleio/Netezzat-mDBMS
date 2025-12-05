namespace mDBMS.Common.Data;

public class DataRetrieval
{
    public string Table { get; set; }
    public string[] Columns { get; set; }
    public IEnumerable<IEnumerable<Condition>>? Condition { get; set; }

    public DataRetrieval(string table, string[] columns, IEnumerable<IEnumerable<Condition>>? condition = null)
    {
        Table = table;
        Columns = columns;
        Condition = condition;
    }
}
