namespace mDBMS.Common.Data;

public class DataWrite(string table, Dictionary<string, object> newValues, IEnumerable<IEnumerable<Condition>>? condition = null)
{
    public string Table { get; set; } = table;
    public Dictionary<string, object> NewValues { get; set; } = newValues;
    public IEnumerable<IEnumerable<Condition>>? Condition { get; set; } = condition;
}
