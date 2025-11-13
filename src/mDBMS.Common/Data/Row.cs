namespace mDBMS.Common.Data;

public class Row
{
    public Dictionary<string, object> Columns { get; set; } = new();

    public object this[string key]
    {
        get => Columns[key];
        set => Columns[key] = value;
    }
}
