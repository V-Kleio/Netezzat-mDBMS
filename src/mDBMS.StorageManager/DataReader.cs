using System;
using System.Collections.Generic;
using System.IO;

namespace mDBMS.StorageManager
{
    // utility class doang si, nampilin .datnya 
    public static class DataReader
    {
        // membaca dan menampilkan seluruh isi file .dat dalam format human-readable
        public static void DisplayFileContent(string filePath)
        {
            // cek apakah file ada
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File {filePath} tidak ditemukan!");
                return;
            }

            Console.WriteLine($"\n=== Membaca File: {filePath} ===\n");

            try
            {
                // baca schema dari header file
                var schema = SchemaSerializer.ReadSchema(filePath);
                
                Console.WriteLine($"Tabel: {schema.TableName}");
                Console.WriteLine($"Jumlah Kolom: {schema.Columns.Count}");
                Console.WriteLine("\nSkema Kolom:");
                foreach (var col in schema.Columns)
                {
                    Console.WriteLine($"  - {col.Name} ({col.Type}, Length: {col.Length})");
                }

                // baca semua data rows dari file
                var rows = ReadAllRows(filePath, schema);
                
                Console.WriteLine($"\nJumlah Baris Data: {rows.Count}");
                Console.WriteLine("\nData:");
                
                // tampilkan header tabel
                Console.Write("| ");
                foreach (var col in schema.Columns)
                {
                    Console.Write($"{col.Name,-20} | ");
                }
                Console.WriteLine();
                
                // tampilkan separator
                Console.Write("|-");
                foreach (var col in schema.Columns)
                {
                    Console.Write(new string('-', 20) + "-|-");
                }
                Console.WriteLine();
                
                // tampilkan setiap baris data
                foreach (var row in rows)
                {
                    Console.Write("| ");
                    foreach (var col in schema.Columns)
                    {
                        var value = row.ContainsKey(col.Name) ? row[col.Name].ToString() : "NULL";
                        Console.Write($"{value,-20} | ");
                    }
                    Console.WriteLine();
                }
                
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error membaca file: {ex.Message}");
            }
        }

        // membaca semua rows dari file .dat
        private static List<Dictionary<string, object>> ReadAllRows(string filePath, TableSchema schema)
        {
            var allRows = new List<Dictionary<string, object>>();

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            
            // skip header - hitung ukuran header
            int headerSize = CalculateHeaderSize(schema);
            fs.Seek(headerSize, SeekOrigin.Begin);

            // baca setiap blok
            byte[] blockBuffer = new byte[BlockSerializer.BlockSize];
            
            while (fs.Position < fs.Length)
            {
                // baca satu blok
                int bytesRead = fs.Read(blockBuffer, 0, BlockSerializer.BlockSize);
                
                if (bytesRead < BlockSerializer.BlockSize)
                {
                    // blok terakhir mungkin tidak penuh
                    Array.Resize(ref blockBuffer, bytesRead);
                }

                // extract rows dari blok
                var blockRows = ReadRowsFromBlock(blockBuffer, schema);
                allRows.AddRange(blockRows);

                // reset buffer untuk blok berikutnya
                if (bytesRead == BlockSerializer.BlockSize)
                {
                    blockBuffer = new byte[BlockSerializer.BlockSize];
                }
            }

            return allRows;
        }

        // membaca rows dari satu blok
        private static List<Dictionary<string, object>> ReadRowsFromBlock(byte[] block, TableSchema schema)
        {
            var rows = new List<Dictionary<string, object>>();

            // baca record count dari blok
            ushort recordCount = BitConverter.ToUInt16(block, 0);
            
            // baca directory offset
            ushort directoryOffset = BitConverter.ToUInt16(block, 2);

            // baca setiap record menggunakan slot directory
            for (int i = 0; i < recordCount; i++)
            {
                // lokasi slot directory untuk record ke-i (dari belakang)
                int slotPos = directoryOffset + (i * 2);
                
                // baca offset data record
                ushort dataOffset = BitConverter.ToUInt16(block, slotPos);
                
                // hitung panjang record
                int recordLength = CalculateRecordLength(schema);
                
                // extract byte array untuk record ini
                byte[] recordBytes = new byte[recordLength];
                Array.Copy(block, dataOffset, recordBytes, 0, recordLength);
                
                // deserialize record menjadi dictionary
                var row = RowSerializer.DeserializeRow(schema, recordBytes);
                rows.Add(row);
            }

            return rows;
        }

        // menghitung ukuran header file
        private static int CalculateHeaderSize(TableSchema schema)
        {
            // 4 bytes magic + 4 bytes version + 20 bytes table name + 4 bytes column count
            int headerSize = 32;
            
            // tambahkan ukuran definisi setiap kolom
            // 20 bytes name + 1 byte type + 4 bytes length = 25 bytes per kolom
            headerSize += schema.Columns.Count * 25;
            
            return headerSize;
        }

        // menghitung panjang satu record berdasarkan schema
        private static int CalculateRecordLength(TableSchema schema)
        {
            int length = 0;
            
            foreach (var col in schema.Columns)
            {
                if (col.Type == DataType.Int)
                {
                    length += 4; // int = 4 bytes
                }
                else if (col.Type == DataType.String)
                {
                    length += col.Length; // varchar = sesuai panjang yang ditentukan
                }
            }
            
            return length;
        }

        // menampilkan raw hex dump dari file (untuk debugging)
        public static void DisplayHexDump(string filePath, int maxBytes = 256)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File {filePath} tidak ditemukan!");
                return;
            }

            Console.WriteLine($"\n=== Hex Dump: {filePath} (first {maxBytes} bytes) ===\n");

            try
            {
                using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                byte[] buffer = new byte[Math.Min(maxBytes, fs.Length)];
                fs.Read(buffer, 0, buffer.Length);

                // tampilkan dalam format hex
                for (int i = 0; i < buffer.Length; i += 16)
                {
                    // offset
                    Console.Write($"{i:X8}  ");
                    
                    // hex values
                    for (int j = 0; j < 16; j++)
                    {
                        if (i + j < buffer.Length)
                        {
                            Console.Write($"{buffer[i + j]:X2} ");
                        }
                        else
                        {
                            Console.Write("   ");
                        }
                        
                        if (j == 7) Console.Write(" ");
                    }
                    
                    Console.Write(" |");
                    
                    // ascii representation
                    for (int j = 0; j < 16 && i + j < buffer.Length; j++)
                    {
                        byte b = buffer[i + j];
                        char c = (b >= 32 && b < 127) ? (char)b : '.';
                        Console.Write(c);
                    }
                    
                    Console.WriteLine("|");
                }
                
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error membaca file: {ex.Message}");
            }
        }

        // menampilkan statistik file
        public static void DisplayFileStats(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File {filePath} tidak ditemukan!");
                return;
            }

            Console.WriteLine($"\n=== Statistik File: {filePath} ===\n");

            try
            {
                var fileInfo = new FileInfo(filePath);
                var schema = SchemaSerializer.ReadSchema(filePath);
                var rows = ReadAllRows(filePath, schema);

                Console.WriteLine($"Nama File: {fileInfo.Name}");
                Console.WriteLine($"Ukuran File: {fileInfo.Length} bytes ({fileInfo.Length / 1024.0:F2} KB)");
                Console.WriteLine($"Nama Tabel: {schema.TableName}");
                Console.WriteLine($"Jumlah Kolom: {schema.Columns.Count}");
                Console.WriteLine($"Jumlah Baris: {rows.Count}");
                
                // hitung jumlah blok
                int headerSize = CalculateHeaderSize(schema);
                int dataSize = (int)fileInfo.Length - headerSize;
                int blockCount = (int)Math.Ceiling((double)dataSize / BlockSerializer.BlockSize);
                
                Console.WriteLine($"Ukuran Header: {headerSize} bytes");
                Console.WriteLine($"Ukuran Data: {dataSize} bytes");
                Console.WriteLine($"Jumlah Blok: {blockCount}");
                Console.WriteLine($"Ukuran per Blok: {BlockSerializer.BlockSize} bytes");
                
                // hitung tuple size
                int tupleSize = CalculateRecordLength(schema);
                Console.WriteLine($"Ukuran per Row: {tupleSize} bytes");
                
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error membaca file: {ex.Message}");
            }
        }
    }
}
