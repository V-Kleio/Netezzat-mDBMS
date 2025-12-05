namespace mDBMS.Common.Data
{
    public class Page(string tableName, int blockID, byte[] data, bool isDirty)
    {
        public string TableName { get; set; } = tableName;
        public int BlockID { get; set; } = blockID;
        public byte[] Data { get; set; } = data;
        public bool IsDirty { get; set; } = isDirty;
    }
}

