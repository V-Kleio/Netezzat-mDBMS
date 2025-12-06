using System.Text;

namespace mDBMS.StorageManager
{
    public static class SchemaSerializer
    {
        private const string MagicNumber = "mDBM";
        private const int Version = 1;
        public const int HeaderSize = 4096; // Fixed 4KB

        // Helper untuk menulis string dengan padding ASCII
        private static void WriteFixedString(BinaryWriter writer, string value, int length)
        {
            var bytes = Encoding.ASCII.GetBytes(value);
            Array.Resize(ref bytes, length); // Padding dengan null/0
            writer.Write(bytes);
        }

        // Helper untuk membaca string dan menghilangkan padding null
        private static string ReadFixedString(BinaryReader reader, int length)
        {
            var bytes = reader.ReadBytes(length);
            return Encoding.ASCII.GetString(bytes).TrimEnd('\0');
        }

        public static void WriteSchema(string filePath, TableSchema schema)
        {
            // Gunakan MemoryStream untuk membuat header 4KB penuh di memori
            byte[] headerBytes = new byte[HeaderSize];

            using (var stream = new MemoryStream(headerBytes))
            using (var writer = new BinaryWriter(stream))
            {
                // 1. Magic Number (4 bytes)
                writer.Write(Encoding.ASCII.GetBytes(MagicNumber));

                // 2. Version (4 bytes)
                writer.Write(Version);

                // 3. Table Name (32 bytes - Fixed Length padding)
                WriteFixedString(writer, schema.TableName, 32);

                // 4. Column Count (4 bytes)
                writer.Write(schema.Columns.Count);

                // 5. Column Definitions
                foreach (var col in schema.Columns)
                {
                    // Col Name (32 bytes)
                    WriteFixedString(writer, col.Name, 32);

                    // DataType (1 byte)
                    writer.Write((byte)col.Type);

                    // Length (4 bytes)
                    writer.Write(col.Length);
                }
            }
            // Tulis buffer 4KB penuh ke awal file
            using (var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                fs.Seek(0, SeekOrigin.Begin);
                fs.Write(headerBytes, 0, HeaderSize);
            }
        }

        public static TableSchema ReadSchema(string filePath)
        {
            using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length < HeaderSize) throw new Exception("File corrupted or too small.");

                byte[] headerBytes = new byte[HeaderSize];
                fs.Read(headerBytes, 0, HeaderSize);

                using (var stream = new MemoryStream(headerBytes))
                using (var reader = new BinaryReader(stream))
                {
                    // 1. Validasi Magic Number
                    string magic = Encoding.ASCII.GetString(reader.ReadBytes(4));
                    if (magic != MagicNumber) throw new Exception("Invalid file format (Magic Number mismatch).");

                    reader.ReadInt32(); // Skip Version

                    // 2. Table Name (32 bytes)
                    string tableName = ReadFixedString(reader, 32);

                    // 3. Column Count
                    int colCount = reader.ReadInt32();

                    var schema = new TableSchema { TableName = tableName };

                    // 4. Read Columns
                    for (int i = 0; i < colCount; i++)
                    {
                        string colName = ReadFixedString(reader, 32);
                        DataType type = (DataType)reader.ReadByte();
                        int len = reader.ReadInt32();

                        schema.Columns.Add(new ColumnSchema { Name = colName, Type = type, Length = len });
                    }
                    return schema;
                }
            }
        }
    }
}
