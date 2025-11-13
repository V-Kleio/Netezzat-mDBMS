using System;
using System.Collections.Generic;
using System.Text;

namespace mDBMS.StorageManager
{
    public class RowSerializer
    {
        public static byte[] SerializeRow(TableSchema schema, Dictionary<string, object> row)
        {
            var buffer = new List<byte>();

            foreach (var col in schema.Columns)
            {
                object value = row[col.Name];
                switch (col.Type)
                {
                    case DataType.Int:
                        buffer.AddRange(BitConverter.GetBytes((int)value));
                        break;
                    case DataType.String:
                        var strBytes = Encoding.ASCII.GetBytes((string)value);
                        Array.Resize(ref strBytes, col.Length);
                        buffer.AddRange(strBytes);
                        break;
                    // Tambahkan tipe data lain sesuai kebutuhan
                }
            }

            return buffer.ToArray();
        }

        public static Dictionary<string, object> DeserializeRow(TableSchema schema, byte[] data)
        {
            var row = new Dictionary<string, object>();
            int offset = 0;

            foreach (var col in schema.Columns)
            {
                switch (col.Type)
                {
                    case DataType.Int:
                        row[col.Name] = BitConverter.ToInt32(data, offset);
                        offset += 4;
                        break;
                    case DataType.String:
                        var strBytes = new byte[col.Length];
                        Array.Copy(data, offset, strBytes, 0, col.Length);
                        row[col.Name] = Encoding.ASCII.GetString(strBytes).TrimEnd('\0');
                        offset += col.Length;
                        break;
                    // Tambahkan tipe data lain sesuai kebutuhan
                }
            }

            return row;
        }
    }
}