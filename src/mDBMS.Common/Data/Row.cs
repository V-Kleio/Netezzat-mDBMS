namespace mDBMS.Common.Data;

public class Row
{
    public required string id;

    public Dictionary<string, object> Columns { get; set; } = [];

    public object this[string key]
    {
        get => Columns[key];
        set => Columns[key] = value;
    }
}
