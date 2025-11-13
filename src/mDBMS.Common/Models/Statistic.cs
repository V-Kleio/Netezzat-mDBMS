namespace mDBMS.Common.Models;

public class Statistic
{
    public string Table { get; set; } = string.Empty;
    public int TupleCount { get; set; }
    public int BlockCount { get; set; }
    public int TupleSize { get; set; }
    public int BlockingFactor { get; set; }
    public int DistinctValues { get; set; }
    public IEnumerable<(string, IndexType)> Indices { get; set; } = [];
}