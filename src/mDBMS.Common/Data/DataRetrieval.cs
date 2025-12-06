namespace mDBMS.Common.Data;

public class DataRetrieval(string table, string[] columns, IEnumerable<IEnumerable<Condition>>? condition = null)
{
    public string Table { get; set; } = table;
    public string[] Columns { get; set; } = columns;
    public IEnumerable<IEnumerable<Condition>>? Condition { get; set; } = condition;
}
