using mDBMS.Common.Data;
namespace mDBMS.QueryProcessor.DML;

class LocalTableStorage
{
    public Dictionary<string, IList<Row>> Tables { get; set; } = new();

    public IList<Row> this[string key]
    {
        get => Tables[key];
        set => Tables[key] = value;
    }
}