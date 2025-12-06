using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class DataReader
    {
        private const int BlockSize = 4096;
        private const int FileHeaderSize = 4096;

        public static void ReadFile(string tableName)
        {
            string filePath = $"{tableName.ToLower()}.dat";
            // Cek path di bin/debug
            string fullPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, filePath);

            if (!File.Exists(fullPath))
            {
                Console.WriteLine($"[ERROR] File {fullPath} tidak ditemukan.");
                return;
            }

            Console.WriteLine($"=== Membaca File: {fullPath} ===");

            TableSchema schema;
            try
            {
                schema = SchemaSerializer.ReadSchema(fullPath);
                Console.WriteLine($"[INFO] Skema terbaca: {schema.TableName} ({schema.Columns.Count} kolom)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Gagal membaca header: {ex.Message}");
                return;
            }

            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                if (fs.Length <= FileHeaderSize)
                {
                    Console.WriteLine("File kosong (hanya header).");
                    return;
                }

                fs.Seek(FileHeaderSize, SeekOrigin.Begin);
                byte[] buffer = new byte[BlockSize];
                int bytesRead;
                int blockIndex = 0;

                while ((bytesRead = fs.Read(buffer, 0, BlockSize)) > 0)
                {
                    if (bytesRead < BlockSize) Array.Clear(buffer, bytesRead, BlockSize - bytesRead);

                    Console.WriteLine($"\n--- Blok Data {blockIndex} ---");
                    try
                    {
                        List<Row> rows = BlockSerializer.DeserializeBlock(schema, buffer);
                        if (rows.Count == 0) Console.WriteLine("(Blok Kosong)");

                        int i = 0;
                        foreach (var row in rows)
                        {
                            Console.Write($"[{i++}] ");
                            foreach (var col in row.Columns) Console.Write($"{col.Key}: {col.Value} | ");
                            Console.WriteLine();
                        }
                    }
                    catch (Exception ex) { Console.WriteLine($"Error blok: {ex.Message}"); }

                    blockIndex++;
                }
            }
            Console.WriteLine("\n=== Selesai ===");
        }
    }
}
