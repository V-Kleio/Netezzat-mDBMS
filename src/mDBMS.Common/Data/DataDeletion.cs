namespace mDBMS.Common.Data;

public class DataDeletion(string table, IEnumerable<IEnumerable<Condition>>? condition = null)
{
    public string Table { get; set; } = table;
    public IEnumerable<IEnumerable<Condition>>? Condition { get; set; } = condition;
}
