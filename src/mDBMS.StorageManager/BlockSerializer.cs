using System;
using System.Collections.Generic;
using System.IO;
using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class BlockSerializer
    {
        public const int BlockSize = 4096;

        // STRUKTUR BLOK:
        // -----------------------------------------------------------
        // | N (2B) | DirStart (2B) | ... DATA RECORD ... | ... DIRECTORY ... |
        // -----------------------------------------------------------
        // [0-1]: Jumlah Record (N)
        // [2-3]: Offset Awal Directory (Pointer ke belakang)
        // [4..]: Data Record ditulis maju (Sequential)
        // [..4096]: Slot Directory ditulis mundur (Pointer ke Data)

        public static byte[] CreateBlock(List<byte[]> rows)
        {
            byte[] block = new byte[BlockSize];
            int recordCount = rows.Count;

            // 1. Header Blok: Jumlah Record
            BitConverter.GetBytes((ushort)recordCount).CopyTo(block, 0);

            int dataPtr = 4; // Data mulai setelah header (byte ke-4)
            int directoryPtr = BlockSize; // Directory mulai dari paling belakang

            for (int i = 0; i < recordCount; i++)
            {
                byte[] rowBytes = rows[i];
                int rowLength = rowBytes.Length;

                // Tulis Data
                Buffer.BlockCopy(rowBytes, 0, block, dataPtr, rowLength);

                // Tulis Directory Entry (Pointer ke awal data record ini)
                directoryPtr -= 2;
                BitConverter.GetBytes((ushort)dataPtr).CopyTo(block, directoryPtr);

                dataPtr += rowLength;
            }

            // 2. Header Blok: Update Offset Awal Directory
            BitConverter.GetBytes((ushort)directoryPtr).CopyTo(block, 2);

            return block;
        }

        public static void AppendBlockToFile(string path, byte[] block)
        {
            using var fs = new FileStream(path, FileMode.Append, FileAccess.Write);
            fs.Write(block, 0, block.Length);
        }

        public static List<Row> DeserializeBlock(TableSchema schema, byte[] blockData)
        {
            var rows = new List<Row>();
            ushort recordCount = BitConverter.ToUInt16(blockData, 0);

            if (recordCount == 0) return rows;

            int currentDirPtr = BlockSize;

            // Iterasi berdasarkan jumlah record di Header
            for (int i = 0; i < recordCount; i++)
            {
                // Baca Pointer dari Directory (Mundur dari belakang)
                currentDirPtr -= 2;
                ushort dataOffset = BitConverter.ToUInt16(blockData, currentDirPtr);

                if (dataOffset >= BlockSize || dataOffset < 4) continue; // Validasi

                // Hitung ukuran row (Fixed Length Schema)
                int rowSize = CalculateRowSize(schema);

                // Ekstrak Byte Row
                byte[] rowBytes = new byte[rowSize];
                Array.Copy(blockData, dataOffset, rowBytes, 0, rowSize);

                // Deserialize menjadi Objek
                Row row = RowSerializer.DeserializeRow(schema, rowBytes);
                rows.Add(row);
            }

            return rows;
        }

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