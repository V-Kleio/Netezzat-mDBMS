using Xunit;
using Xunit.Abstractions; //untuk logging
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
    public class StorageEngineTests : IDisposable
    {
        private readonly string _enginePath;
        private readonly ITestOutputHelper _output; // Variabel  logging

        // Inject ITestOutputHelper ke Constructor
        public StorageEngineTests(ITestOutputHelper output)
        {
            _output = output;

            // Setup Path
            var fieldInfo = typeof(StorageEngine).GetField("DataPath", BindingFlags.Static | BindingFlags.NonPublic);
            _enginePath = (string?)fieldInfo?.GetValue(null) ?? Path.Combine(Directory.GetCurrentDirectory(), "data");

            if (!Directory.Exists(_enginePath))
            {
                Directory.CreateDirectory(_enginePath);
                _output.WriteLine($"[SETUP] Membuat folder data sementara di: {_enginePath}");
            }
            else
            {
                _output.WriteLine($"[SETUP] Menggunakan folder data existing di: {_enginePath}");
            }
        }

        [Fact]
        public void WriteDisk_ShouldWritePageData_ToCorrectOffset()
        {
            _output.WriteLine(">>> TEST 1: WriteDisk (Low Level Write) Dimulai");

            // Arrange
            var storage = new StorageEngine(null);
            string tableName = "UnitTestTable";
            string fileName = "unittesttable.dat";
            int blockId = 0;
            byte[] data = new byte[4096];
            Array.Fill(data, (byte)0xAB);
            var page = new Page(tableName, blockId, data, true);

            _output.WriteLine($"[ARRANGE] Menyiapkan Page dummy penuh dengan byte 0xAB untuk tabel '{tableName}'");

            // Act
            int result = storage.WriteDisk(page);
            _output.WriteLine($"[ACT] Menjalankan storage.WriteDisk(). Hasil return: {result}");

            // Assert
            Assert.Equal(1, result);
            string fullPath = Path.Combine(_enginePath, fileName);
            Assert.True(File.Exists(fullPath));
            
            _output.WriteLine($"[ASSERT] Sukses! File fisik ditemukan di {fullPath}");
            _output.WriteLine(">>> TEST 1 SELESAI \n");
        }

        [Fact]
        public void AddBlock_ShouldCreateFile_AndInsertRow()
        {
            _output.WriteLine(">>> TEST 2: AddBlock (INSERT Data Baru) Dimulai");

            // Arrange
            string tableName = "StudentTest";
            string fileName = "studenttest.dat";
            string fullPath = Path.Combine(_enginePath, fileName);

            var schema = new TableSchema { TableName = tableName };
            schema.Columns.Add(new ColumnSchema { Name = "Name", Type = DataType.String, Length = 50 });
            schema.Columns.Add(new ColumnSchema { Name = "Age", Type = DataType.Int, Length = 4 });
            SchemaSerializer.WriteSchema(fullPath, schema);
            _output.WriteLine("[ARRANGE] Schema 'StudentTest' berhasil ditulis ke disk.");

            var mockBuffer = new Mock<IBufferManager>();
            StorageEngine storage = null!;
            mockBuffer.Setup(b => b.ReadFromBuffer(It.IsAny<string>(), It.IsAny<int>())).Returns((byte[]?)null);
            mockBuffer.Setup(b => b.WriteToBuffer(It.IsAny<Page>())).Callback<Page>(p => storage.WriteDisk(p));
            storage = new StorageEngine(mockBuffer.Object);

            var newValues = new Dictionary<string, object> { { "Name", "Budi" }, { "Age", 20 } };
            var dataWrite = new DataWrite(tableName, newValues, new List<List<Condition>>());

            // Act
            _output.WriteLine("[ACT] Mencoba INSERT data: Name='Budi', Age=20");
            int result = storage.AddBlock(dataWrite);

            // Assert
            Assert.Equal(1, result);
            long expectedSize = 4096 + 4096; // Header + 1 Block
            using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                Assert.Equal(expectedSize, fs.Length);
            }
            _output.WriteLine($"[ASSERT] Sukses! Ukuran file bertambah menjadi {expectedSize} bytes (Header + Block).");
            _output.WriteLine(">>> TEST 2 SELESAI \n");
        }

        [Fact]
        public void ReadBlock_ShouldRetrieveInsertedRow()
        {
            _output.WriteLine(">>> TEST 3: ReadBlock (SELECT Data) Dimulai");

            // Arrange
            string tableName = "StudentRead";
            string fileName = "studentread.dat";
            string fullPath = Path.Combine(_enginePath, fileName);

            var mockBuffer = new Mock<IBufferManager>();
            StorageEngine storage = null!;
            mockBuffer.Setup(b => b.ReadFromBuffer(It.IsAny<string>(), It.IsAny<int>())).Returns((byte[]?)null);
            mockBuffer.Setup(b => b.WriteToBuffer(It.IsAny<Page>())).Callback<Page>(p => storage.WriteDisk(p));
            storage = new StorageEngine(mockBuffer.Object);

            var schema = new TableSchema { TableName = tableName };
            schema.Columns.Add(new ColumnSchema { Name = "Name", Type = DataType.String, Length = 20 });
            SchemaSerializer.WriteSchema(fullPath, schema);

            var dataWrite = new DataWrite(tableName, new Dictionary<string, object> { { "Name", "Alice" } }, new List<List<Condition>>());
            storage.AddBlock(dataWrite);
            _output.WriteLine("[ARRANGE] Data awal 'Alice' telah di-insert.");

            // Act 
            _output.WriteLine("[ACT] Melakukan ReadBlock untuk kolom 'Name'...");
            var retrieval = new DataRetrieval(tableName, new[] { "Name" }, new List<List<Condition>>());
            var resultRows = storage.ReadBlock(retrieval).ToList();

            // Assert
            Assert.Single(resultRows);
            string retrievedName = (string)resultRows.First()["Name"];
            Assert.Equal("Alice", retrievedName);
            
            _output.WriteLine($"[ASSERT] Data berhasil dibaca: '{retrievedName}'. Cocok dengan ekspektasi.");
            _output.WriteLine(">>> TEST 3 SELESAI \n");
        }

        [Fact]
        public void WriteBlock_ShouldUpdateExistingValue()
        {
            _output.WriteLine(">>> TEST 4: WriteBlock (UPDATE Data) Dimulai");

            // Arrange
            string tableName = "StudentUpdate";
            string fileName = "studentupdate.dat";
            string fullPath = Path.Combine(_enginePath, fileName);

            var mockBuffer = new Mock<IBufferManager>();
            mockBuffer.Setup(b => b.ReadFromBuffer(It.IsAny<string>(), It.IsAny<int>())).Returns((byte[]?)null);
            StorageEngine storage = null!;
            mockBuffer.Setup(b => b.WriteToBuffer(It.IsAny<Page>())).Callback<Page>(p => storage.WriteDisk(p));
            storage = new StorageEngine(mockBuffer.Object);

            var schema = new TableSchema { TableName = tableName };
            schema.Columns.Add(new ColumnSchema { Name = "Score", Type = DataType.Int, Length = 4 });
            SchemaSerializer.WriteSchema(fullPath, schema);

            storage.AddBlock(new DataWrite(tableName, new Dictionary<string, object> { { "Score", 50 } }, new List<List<Condition>>()));
            _output.WriteLine("[ARRANGE] Data awal Score=50 telah di-insert.");

            // Act
            _output.WriteLine("[ACT] Mengupdate Score menjadi 100 dimana Score == 50...");
            var condition = new Condition { lhs = "Score", opr = Condition.Operation.EQ, rhs = (int)50 };
            var conditions = new List<List<Condition>> { new List<Condition> { condition } };
            var updateData = new DataWrite(tableName, new Dictionary<string, object> { { "Score", 100 } }, conditions);
            
            int updatedCount = storage.WriteBlock(updateData);

            // Assert
            Assert.Equal(1, updatedCount);
            
            var readRes = storage.ReadBlock(new DataRetrieval(tableName, null, new List<List<Condition>>())).ToList();
            int actualScore = (int)readRes.First()["Score"];
            Assert.Equal(100, actualScore);
            
            _output.WriteLine($"[ASSERT] Update berhasil. Row count affected: {updatedCount}. Nilai baru di disk: {actualScore}");
            _output.WriteLine(">>> TEST 4 SELESAI \n");
        }

        [Fact]
        public void DeleteBlock_ShouldRemoveRow()
        {
            // Arrange
            string tableName = "StudentDelete";
            string fileName = "studentdelete.dat";
            string fullPath = Path.Combine(_enginePath, fileName);

            var mockBuffer = new Mock<IBufferManager>();
            mockBuffer.Setup(b => b.ReadFromBuffer(It.IsAny<string>(), It.IsAny<int>())).Returns((byte[]?)null);
            
            StorageEngine storage = null!;
            mockBuffer.Setup(b => b.WriteToBuffer(It.IsAny<Page>()))
                      .Callback<Page>(p => storage.WriteDisk(p));
            
            storage = new StorageEngine(mockBuffer.Object);

            var schema = new TableSchema { TableName = tableName };
            schema.Columns.Add(new ColumnSchema { Name = "ID", Type = DataType.Int, Length = 4 });
            SchemaSerializer.WriteSchema(fullPath, schema);

            // Insert 1 baris
            storage.AddBlock(new DataWrite(tableName, new Dictionary<string, object> { { "ID", 1 } }, new List<List<Condition>>()));

            // Delete Baris tersebut
            var condition = new Condition { lhs = "ID", opr = Condition.Operation.EQ, rhs = (int)1 };
            var conditions = new List<List<Condition>> { new List<Condition> { condition } };
            var deleteData = new DataDeletion(tableName, conditions);

            int deletedCount = storage.DeleteBlock(deleteData);

            // Assert
            Assert.Equal(1, deletedCount);

            // Verifikasi data sudah terhapus
            var readRes = storage.ReadBlock(new DataRetrieval(tableName, null, new List<List<Condition>>()));
            Assert.Empty(readRes); 
        }

        public void Dispose()
        {
            try
            {
                var filesToDelete = new[]
                {
                    "unittesttable.dat", "studenttest.dat",
                    "studentread.dat", "studentupdate.dat", "studentdelete.dat"
                };

                foreach (var f in filesToDelete)
                {
                    string path = Path.Combine(_enginePath, f);
                    if (File.Exists(path)) File.Delete(path);
                }
                _output.WriteLine("[CLEANUP] File-file test sementara berhasil dihapus.");
            }
            catch { }
        }
    }
}