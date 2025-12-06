using Xunit;
using Xunit.Abstractions;
using mDBMS.StorageManager;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using System.Reflection;
using System.IO;
using System;
using System.Collections.Generic;
using Moq;
using System.Linq;

namespace mDBMS.StorageManager.Tests
{
    public class SystemIntegrationTests : IDisposable
    {
        private readonly string _enginePath;
        private readonly ITestOutputHelper _output;

        public SystemIntegrationTests(ITestOutputHelper output)
        {
            _output = output;
            // Setup Path
            var fieldInfo = typeof(StorageEngine).GetField("DataPath", BindingFlags.Static | BindingFlags.NonPublic);
            _enginePath = (string?)fieldInfo?.GetValue(null) ?? Path.Combine(Directory.GetCurrentDirectory(), "data");

            if (!Directory.Exists(_enginePath)) Directory.CreateDirectory(_enginePath);
        }

        [Fact]
        public void Buffer_Storage_Integration_Flush_Scenario()
        {
            _output.WriteLine(">>> SYSTEM TEST: Buffer <-> Storage Integration Dimulai");

            // ARRANGE
            string tableName = "SystemTestTable";
            string fileName = "systemtesttable.dat";
            string fullPath = Path.Combine(_enginePath, fileName);

            var schema = new TableSchema { TableName = tableName };
            schema.Columns.Add(new ColumnSchema { Name = "Val", Type = DataType.String, Length = 50 });
            SchemaSerializer.WriteSchema(fullPath, schema);

            byte[] emptyBlock = BlockSerializer.CreateBlock(new List<byte[]>()); // Buat blok kosong valid
            BlockSerializer.AppendBlockToFile(fullPath, emptyBlock);

            long initialSize = new FileInfo(fullPath).Length;
            _output.WriteLine($"[INIT] Ukuran file awal (Header + Empty Block): {initialSize} bytes");

            //  Simulasi Buffer Manager (RAM)
            var ramBuffer = new Dictionary<int, byte[]>();
            var mockBuffer = new Mock<IBufferManager>();
            StorageEngine storage = null!;

            // WriteToBuffer: Simpan ke Dictionary
            mockBuffer.Setup(b => b.WriteToBuffer(It.IsAny<Page>()))
                      .Callback<Page>(p => 
                      {
                          ramBuffer[p.BlockID] = p.Data; 
                          _output.WriteLine($"[BUFFER] Page {p.BlockID} disimpan di Memory (Dirty Page).");
                      });

            // ReadFromBuffer: Baca dari Dictionary
            mockBuffer.Setup(b => b.ReadFromBuffer(It.IsAny<string>(), It.IsAny<int>()))
                      .Returns<string, int>((tbl, id) => 
                      {
                          if (ramBuffer.ContainsKey(id)) return ramBuffer[id];
                          return null; // Buffer Miss
                      });

            storage = new StorageEngine(mockBuffer.Object);

            // ACT - Insert Data (yang akan masuk ke Buffer dulu)
            // Insert data ke tabel. Karena disk punya Block 0 (kosong), AddBlock akan membaca blok itu,
            // mengisi data, lalu menyimpannya ke Buffer (RAM).
            var dataWrite = new DataWrite(tableName, new Dictionary<string, object> { { "Val", "DataPending" } }, new List<List<Condition>>());
            
            _output.WriteLine("[ACT] Melakukan Insert Data 'DataPending'...");
            storage.AddBlock(dataWrite);

            // ASSERTION 1 (Cek Disk vs Memori)
            
            // Cek 1: ReadBlock harus BERHASIL baca data baru (Hit dari Buffer)
            // Sekarang berhasil karena totalBlocks > 0 (file fisik ada blok kosongnya)
            var readRes = storage.ReadBlock(new DataRetrieval(tableName, null, new List<List<Condition>>())).ToList();
            Assert.Single(readRes);
            Assert.Equal("DataPending", readRes.First()["Val"]);
            _output.WriteLine("[SUCCESS] Data berhasil dibaca kembali (Read hit dari Buffer).");

            // Cek 2: File di Disk isinya harus Masih Kosong (Belum ada "DataPending")
            // Baca manual file fisiknya
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                byte[] diskBlock = new byte[4096];
                fs.Seek(4096, SeekOrigin.Begin);
                fs.Read(diskBlock, 0, 4096);
                
                // Deserialize blok dari disk. Harusnya Kosong (0 rows).
                var rowsOnDisk = BlockSerializer.DeserializeBlock(schema, diskBlock);
                Assert.Empty(rowsOnDisk); // <-- BUKTI: Data belum dipersist ke disk!
            }
            _output.WriteLine("[SUCCESS] Data fisik di disk masih kosong (Buffer-First terbukti).");

            // ACT - SIMULASI FLUSH
            _output.WriteLine("[ACT] Simulasi FLUSH / Checkpoint...");
            foreach(var kvp in ramBuffer)
            {
                var page = new Page(tableName, kvp.Key, kvp.Value, false);
                storage.WriteDisk(page); // Flush ke disk
            }

            // ASSERTION 2 (Final Persistence)
            // Sekarang baca lagi dari disk, harusnya sudah ada datanya
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read))
            {
                byte[] diskBlock = new byte[4096];
                fs.Seek(4096, SeekOrigin.Begin);
                fs.Read(diskBlock, 0, 4096);
                
                var rowsOnDisk = BlockSerializer.DeserializeBlock(schema, diskBlock);
                Assert.Single(rowsOnDisk);
                Assert.Equal("DataPending", rowsOnDisk[0]["Val"]);
            }
            
            _output.WriteLine("[SUCCESS] Data sekarang permanen di disk setelah Flush.");
            _output.WriteLine(">>> SYSTEM TEST SELESAI \n");
        }

        public void Dispose()
        {
            try
            {
                string path = Path.Combine(_enginePath, "systemtesttable.dat");
                if (File.Exists(path)) File.Delete(path);
            }
            catch { }
        }
    }
}