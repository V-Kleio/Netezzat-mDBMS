using System;
using System.Collections.Generic;
using System.IO;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class BlockSerializer
    {
        public const int BlockSize = 4096; // jadi kita asumsi nya 1 block 4KB yak, kalo diubah disini berarti


        // Format Blok Datanya
        // [0-1]:     jumlah Record (ushort)
        // [2-3]:     Offset Awal Directory (ushort)
        // [4-...]:   Data Record (ditulis maju)
        // [...-end]: Slot Directory (ditulis belakang)
        public static byte[] CreateBlock(List<byte[]> rows)
        {
            byte[] block = new byte[BlockSize];
            int recordCount = rows.Count;

            // tulis jumlah record
            BitConverter.GetBytes((ushort)recordCount).CopyTo(block, 0);

            // pointer awal data
            int dataPtr = 4;
            int directoryPtr = BlockSize;

            // tulis tiap record
            for (int i = 0; i < recordCount; i++)
            {
                byte[] rowBytes = rows[i];
                int rowLength = rowBytes.Length;

                // simpan data record
                Buffer.BlockCopy(rowBytes, 0, block, dataPtr, rowLength);

                // update directory pointer (di bagian belakang blok)
                directoryPtr -= 2;
                BitConverter.GetBytes((ushort)dataPtr).CopyTo(block, directoryPtr);

                // geser pointer data
                dataPtr += rowLength;
            }

            // tulis offset awal slot directory
            BitConverter.GetBytes((ushort)directoryPtr).CopyTo(block, 2);

            return block;
        }

        public static void AppendBlockToFile(string path, byte[] block)
        {
            using var fs = new FileStream(path, FileMode.Append, FileAccess.Write);
            fs.Write(block, 0, block.Length);
        }

        // Method baru untuk Membaca Blok (Deserialisasi)
        public static List<Row> DeserializeBlock(TableSchema schema, byte[] blockData)
        {
            var rows = new List<Row>();

            // 1. Baca Header Blok
            // [0-1]: Jumlah Record
            ushort recordCount = BitConverter.ToUInt16(blockData, 0);
            
            // [2-3]: Offset Directory (tidak terlalu dipakai saat read linear, tapi berguna validasi)
            // ushort dirStart = BitConverter.ToUInt16(blockData, 2);

            // Jika kosong, return list kosong
            if (recordCount == 0) return rows;

            // 2. Baca Slot Directory untuk mendapatkan pointer data
            // Directory ditulis dari BELAKANG blok.
            // Slot 0 ada di: BlockSize - 2
            // Slot 1 ada di: BlockSize - 4
            // dst...
            
            int currentDirPtr = BlockSize;

            for (int i = 0; i < recordCount; i++)
            {
                // Mundur 2 byte untuk baca pointer slot ke-i
                currentDirPtr -= 2;
                ushort dataOffset = BitConverter.ToUInt16(blockData, currentDirPtr);

                // Validasi offset (opsional, agar tidak crash jika korup)
                if (dataOffset >= BlockSize || dataOffset < 4) continue;

                // 3. Menentukan panjang data record
                // Karena kita pakai Fixed-Length Schema (untuk M2), kita bisa hitung size row dari schema
                // TAPI: Slotted page murni biasanya menyimpan panjang row di header slot.
                // UNTUK SIMPLIFIKASI M2: Kita hitung panjang row berdasarkan schema column definitions.
                int rowSize = CalculateRowSize(schema);

                // 4. Ambil slice byte untuk row ini
                byte[] rowBytes = new byte[rowSize];
                Array.Copy(blockData, dataOffset, rowBytes, 0, rowSize);

                // 5. Deserialisasi Row
                Row row = RowSerializer.DeserializeRow(schema, rowBytes);
                rows.Add(row);
            }

            return rows;
        }
        // Helper untuk hitung ukuran byte 1 baris berdasarkan schema
        public static int CalculateRowSize(TableSchema schema)
        {
            int size = 0;
            foreach (var col in schema.Columns)
            {
                size += col.Type switch
                {
                    DataType.Int => 4,
                    DataType.Float => 4,
                    DataType.String => col.Length,
                    _ => 0
                };
            }
            return size;
        }

    }
}