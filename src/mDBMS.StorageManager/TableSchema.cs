using System.Collections.Generic;

namespace mDBMS.StorageManager
{
    public class TableSchema
    {
        public string TableName { get; set; } = string.Empty;
        public List<ColumnSchema> Columns { get; set; } = new();

    }
}