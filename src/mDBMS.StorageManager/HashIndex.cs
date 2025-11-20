using System.Collections.Generic;

namespace mDBMS.StorageManager
{
    public class HashIndex
    {
        // Key: Value Kolom (misal: 101, "Budi")
        // Value: List of BlockOffsets (misal: [4096, 8192])
        private readonly Dictionary<object, List<long>> _index = new();
        
        public string TableName { get; }
        public string ColumnName { get; }

        public HashIndex(string tableName, string columnName)
        {
            TableName = tableName;
            ColumnName = columnName;
        }

        public void Add(object key, long blockOffset)
        {
            if (key == null) return;

            if (!_index.ContainsKey(key))
            {
                _index[key] = new List<long>();
            }
            
            // Hindari duplikat offset (meskipun jarang terjadi di append-only)
            if (!_index[key].Contains(blockOffset))
            {
                _index[key].Add(blockOffset);
            }
        }

        public List<long>? GetBlockOffsets(object key)
        {
            if (key == null) return null;
            _index.TryGetValue(key, out var offsets);
            return offsets;
        }
    }
}