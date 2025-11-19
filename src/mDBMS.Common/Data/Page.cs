using System;
namespace mDBMS.Common.Data
{
    public class Page
    {
        public string TableName { get; set; }
        public int BlockID { get; set; }
        public byte[] Data { get; set; } = new byte[4096];
        public bool IsDirty { get; set; } = false; 
    }
}