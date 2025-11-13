using System;
using System.Collections.Generic;
using System.IO;


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
 
    }
}