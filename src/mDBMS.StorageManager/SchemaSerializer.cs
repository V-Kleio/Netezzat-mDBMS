using System.Text ; 

namespace mDBMS.StorageManager
{
    public class SchemaSerializer
    {
        public static void WriteSchema(string path, TableSchema schema)
        {
            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
            using var bw = new BinaryWriter(fs, Encoding.UTF8);

            // magic number
            bw.Write(Encoding.ASCII.GetBytes("mDBM"));
            bw.Write(1);
            // nama tabel
            WriteFixedString(bw, schema.TableName, 20);
            // jumlah kolom
            bw.Write(schema.Columns.Count);
            // definisi kolom
            foreach (var col in schema.Columns)
            {
                WriteFixedString(bw, col.Name, 20);
                bw.Write((byte)col.Type);
                bw.Write(col.Length);
            }
        }

        private static void WriteFixedString(BinaryWriter bw, string value, int length)
        {
            var bytes = Encoding.ASCII.GetBytes(value);
            Array.Resize(ref bytes, length);
            bw.Write(bytes);
        }

        public static TableSchema ReadSchema(string path)
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
            using var br = new BinaryReader(fs, Encoding.UTF8);

            // format header kek gini
            // Byte 0-3:   Magic Number "mDBM"
            // Byte 4-7:   Version (int32)
            // Byte 8-27:  Nama Tabel (20 byte fixed)
            // Byte 28-31: Jumlah Kolom (int32)
            // Untuk setiap kolom:
            //   - 20 byte: Nama Kolom
            //   - 1 byte:   Tipe Data
            //   - 4 byte:  Panjang
            

            var magic = Encoding.ASCII.GetString(br.ReadBytes(4));
            if (magic != "mDBM")
                throw new Exception("Invalid file format.");

            int version = br.ReadInt32();
            string tableName = ReadFixedString(br, 20);
            int columnCount = br.ReadInt32();

            var columns = new List<ColumnSchema>();
            for (int i = 0; i < columnCount; i++)
            {
                string colName = ReadFixedString(br, 20);
                var type = (DataType)br.ReadByte();
                int length = br.ReadInt32();

                columns.Add(new ColumnSchema
                {
                    Name = colName,
                    Type = type,
                    Length = length
                });
            }

            return new TableSchema { TableName = tableName, Columns = columns };
        }

        private static string ReadFixedString(BinaryReader br, int length)
        {
            var bytes = br.ReadBytes(length);
            return Encoding.ASCII.GetString(bytes).TrimEnd('\0');
        }

    }
    
}
