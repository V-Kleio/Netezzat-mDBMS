namespace mDBMS.StorageManager
{
    public class ColumnSchema
    {
        public string Name { get; set; } = string.Empty;
        public DataType Type { get; set; }
        public int Length { get; set; }
 
    }
}