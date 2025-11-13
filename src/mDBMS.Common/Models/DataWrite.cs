using System.Collections.Generic;
namespace mDBMS.Common.Models;

public class DataWrite
{
    public string Table { get; set; }
    public Dictionary<string, object> NewValues { get; set; }
    public Condition? Condition { get; set; }

    public DataWrite(string table, Dictionary<string, object> newValues, Condition? condition = null)
    {
        Table = table;
        NewValues = newValues;
        Condition = condition;
    }
}