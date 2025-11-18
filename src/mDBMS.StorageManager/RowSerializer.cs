using System;
using System.Collections.Generic;
using System.Text;
using mDBMS.Common.Data; // Pastikan using ini ada untuk akses Row

namespace mDBMS.StorageManager
{
    public static class RowSerializer
    {
        public static byte[] SerializeRow(TableSchema schema, Row row)
        {
            var buffer = new List<byte>();

            foreach (var col in schema.Columns)
            {
                // Ambil value, jika tidak ada default ke null/0
                object? value = row.Columns.ContainsKey(col.Name) ? row[col.Name] : null;

                switch (col.Type)
                {
                    case DataType.Int:
                        int intVal = value is int i ? i : 0;
                        buffer.AddRange(BitConverter.GetBytes(intVal));
                        break;

                    case DataType.Float: // TAMBAHAN: Support Float (GPA)
                        float floatVal = value is float f ? f : 0.0f;
                        // Jika input double (dari JSON/C# default), cast ke float
                        if (value is double d) floatVal = (float)d;
                        buffer.AddRange(BitConverter.GetBytes(floatVal));
                        break;

                    case DataType.String:
                        string strVal = value as string ?? string.Empty;
                        // Potong jika kepanjangan, atau biarkan jika pas
                        if (strVal.Length > col.Length) strVal = strVal.Substring(0, col.Length);
                        
                        var strBytes = new byte[col.Length]; // Array otomatis terisi 0 (padding)
                        Encoding.ASCII.GetBytes(strVal).CopyTo(strBytes, 0);
                        buffer.AddRange(strBytes);
                        break;
                }
            }
            return buffer.ToArray();
        }

        public static Row DeserializeRow(TableSchema schema, byte[] data)
        {
            var row = new Row();
            int offset = 0;

            foreach (var col in schema.Columns)
            {
                switch (col.Type)
                {
                    case DataType.Int:
                        row[col.Name] = BitConverter.ToInt32(data, offset);
                        offset += 4;
                        break;

                    case DataType.Float: // TAMBAHAN: Support Float
                        row[col.Name] = BitConverter.ToSingle(data, offset);
                        offset += 4;
                        break;

                    case DataType.String:
                        // Baca sepanjang col.Length
                        string str = Encoding.ASCII.GetString(data, offset, col.Length);
                        row[col.Name] = str.TrimEnd('\0'); // Hapus padding null
                        offset += col.Length;
                        break;
                }
            }
            return row;
        }
    }
}