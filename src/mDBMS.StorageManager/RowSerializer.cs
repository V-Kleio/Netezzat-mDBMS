using System.Text;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class RowSerializer
    {
        public static byte[] SerializeRow(TableSchema schema, Row row)
        {

            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                string rowId = row.id ?? Guid.NewGuid().ToString();
                writer.Write(rowId);

                foreach (var col in schema.Columns)
                {
                    object? value = row.Columns.ContainsKey(col.Name) ? row[col.Name] : null;

                    switch (col.Type)
                    {
                        case DataType.Int:
                            int intVal = value is int i ? i : 0;
                            writer.Write(intVal);
                            break;

                        case DataType.Float:
                            float floatVal = value is float f ? f : 0.0f;
                            if (value is double d) floatVal = (float)d;
                            writer.Write(floatVal);
                            break;

                        case DataType.String:
                            string strVal = value as string ?? string.Empty;
                            if (strVal.Length > col.Length)
                            {
                                Console.WriteLine($"[WARNING] String truncated for column '{col.Name}': '{strVal}' -> '{strVal[..col.Length]}'");
                                strVal = strVal[..col.Length];
                            }

                            byte[] strBytes = new byte[col.Length];
                            Encoding.ASCII.GetBytes(strVal).CopyTo(strBytes, 0);
                            writer.Write(strBytes);
                            break;
                    }
                }

                return ms.ToArray();
            }
        }

        public static Row DeserializeRow(TableSchema schema, byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var reader = new BinaryReader(ms))
            {
                var row = new Row();

                try
                {
                    row.id = reader.ReadString();
                }
                catch
                {

                    row.id = Guid.NewGuid().ToString();
                }

                foreach (var col in schema.Columns)
                {
                    switch (col.Type)
                    {
                        case DataType.Int:
                            row[col.Name] = reader.ReadInt32();
                            break;

                        case DataType.Float:
                            row[col.Name] = reader.ReadSingle();
                            break;

                        case DataType.String:
                            byte[] strBytes = reader.ReadBytes(col.Length);
                            string str = Encoding.ASCII.GetString(strBytes);
                            row[col.Name] = str.TrimEnd('\0');
                            break;
                    }
                }
                return row;
            }
        }
    }
}
