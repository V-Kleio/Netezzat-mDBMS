using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class LocalTableStorage
{
    public Dictionary<string, IList<Row>> Columns { get; set; } = new();

    public IList<Row> this[string key]
    {
        get => Columns[key];
        set => Columns[key] = value;
    }
}