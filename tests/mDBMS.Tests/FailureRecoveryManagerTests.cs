using Xunit;
using Xunit.Abstractions;
using mDBMS.FailureRecovery;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Transaction;
using mDBMS.Common.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace mDBMS.Tests
{

    // DUMMY CLASSES (MOCKS)
    public class MockQueryProcessor : IQueryProcessor
    {
        public string LastExecutedQuery { get; private set; } = string.Empty;

        public ExecutionResult ExecuteQuery(string query, int transactionId)
        {
            LastExecutedQuery = query;
            return new ExecutionResult { Success = true, Message = "Mock Success" };
        }
    }

    public class MockStorageManager : IStorageManager
    {
        public int WriteDisk(Page page) => 1; 
        public int AddBlock(DataWrite data) => 1;
        public int WriteBlock(DataWrite data) => 1;
        public IEnumerable<Row> ReadBlock(DataRetrieval data) => new List<Row>();
        public int DeleteBlock(DataDeletion data) => 1;
        public void SetIndex(string t, string c, IndexType type) { }
        public Statistic GetStats(string t) => new Statistic();
    }

    // UNIT TEST CLASS
    public class FailureRecoveryManagerTests : IDisposable
    {
        private readonly string _logDir;
        private readonly string _logFile;
        private readonly ITestOutputHelper _output;

        public FailureRecoveryManagerTests(ITestOutputHelper output)
        {
            _output = output;
            _logDir = Path.Combine(Directory.GetCurrentDirectory(), "logs");
            _logFile = Path.Combine(_logDir, "mDBMS.log");
            Cleanup(); 
        }
        public void Dispose()
        {
            Cleanup();
        }

        private void Cleanup()
        {
            if (Directory.Exists(_logDir))
            {
                try { Directory.Delete(_logDir, true); } catch { }
            }
        }

        // TEST 1: Data Logging (INSERT/UPDATE/DELETE)
        // Tujuan: Memastikan operasi data biasa tercatat di log dan LSN bertambah.
        [Fact]
        public void Test1_WriteLog_ShouldIncrementLSN_AndCreateFile()
        {
            _output.WriteLine("SQL: WriteLog(INSERT)");
            
            var frm = new FailureRecoveryManager(null, null);
            long initialLSN = frm.GetCurrentLSN();
            _output.WriteLine($"[Setup] Initial LSN: {initialLSN}");

            var insertLog = new ExecutionLog
            {
                Operation = ExecutionLog.OperationType.INSERT,
                TransactionId = 1,
                TableName = "Users",
                RowIdentifier = "ID=1",
                AfterImage = new Row { Columns = { { "Name", "Budi" } } }
            };

            frm.WriteLog(insertLog);
            _output.WriteLine("[Action] Log written to buffer");
            
            frm.FlushLogBuffer(); // Paksa tulis ke file biar bisa dicek
            _output.WriteLine("[Action] Buffer flushed to disk");

            // Assert
            // Cek LSN naik
            long currentLSN = frm.GetCurrentLSN();
            Assert.Equal(initialLSN + 1, currentLSN);
            _output.WriteLine($"[SUCCESS] LSN Incremented (Expected: {initialLSN + 1}, Actual: {currentLSN})");

            // Cek File Terbuat
            Assert.True(File.Exists(_logFile), "File log harusnya terbentuk.");
            _output.WriteLine("[SUCCESS] Log file created successfully");

            // Cek Isi File mengandung kata kunci
            string content = File.ReadAllText(_logFile);
            Assert.Contains("INSERT", content);
            Assert.Contains("Users", content);
            _output.WriteLine("[SUCCESS] Log content verification passed (Contains 'INSERT' & 'Users')");
            
            _output.WriteLine("SUCCESS");
        }

        // TEST 2: Transaction Control (BEGIN/COMMIT/ABORT)
        // Tujuan: Memastikan flow transaksi tercatat.
        [Fact]
        public void Test2_TransactionControl_ShouldLogCorrectly()
        {
            _output.WriteLine("SQL: Transaction Control Flow");
            var frm = new FailureRecoveryManager(null, null);

            // Tulis BEGIN
            var beginEntry = LogEntry.CreateBeginTransaction(frm.GetCurrentLSN(), 100);
            frm.WriteLogEntry(beginEntry);
            _output.WriteLine("[Action] BEGIN_TRANSACTION logged");

            // Tulis COMMIT
            var commitEntry = LogEntry.CreateCommit(frm.GetCurrentLSN(), 100);
            frm.WriteLogEntry(commitEntry);
            _output.WriteLine("[Action] COMMIT logged");

            // Assert
            string[] lines = File.ReadAllLines(_logFile);
            Assert.Equal(2, lines.Length);
            _output.WriteLine($"[SUCCESS] Log file contains {lines.Length} entries");

            Assert.Contains("BEGIN_TRANSACTION", lines[0]);
            _output.WriteLine("[SUCCESS] Entry 1 matches: BEGIN_TRANSACTION");
            
            Assert.Contains("COMMIT", lines[1]);
            _output.WriteLine("[SUCCESS] Entry 2 matches: COMMIT");
            
            _output.WriteLine("SUCCESS");
        }

        // TEST 3: Buffer Operations
        // Tujuan: Memastikan buffer pool menyimpan dan mengembalikan data.
        [Fact]
        public void Test3_Buffer_ReadWrite()
        {
            _output.WriteLine("SQL: Buffer Read/Write Operations");
            var frm = new FailureRecoveryManager(new MockQueryProcessor(), new MockStorageManager());
            
            byte[] dummyData = new byte[4096];
            dummyData[0] = 99; // Penanda
            var page = new Page("Mahasiswa", 1, dummyData, true);
            _output.WriteLine("[Setup] Page created with data[0]=99");

            frm.WriteToBuffer(page);
            _output.WriteLine("[Action] WriteToBuffer executed");
            
            byte[] retrievedData = frm.ReadFromBuffer("Mahasiswa", 1);
            _output.WriteLine("[Action] ReadFromBuffer executed");

            // Assert
            Assert.NotNull(retrievedData);
            Assert.NotEmpty(retrievedData);
            _output.WriteLine("[SUCCESS] Data retrieved is not null/empty");
            
            Assert.Equal(99, retrievedData[0]);
            _output.WriteLine($"[SUCCESS] Data integrity check passed (Value: {retrievedData[0]})");
            
            _output.WriteLine("SUCCESS");
        }

        // TEST 4: Periodic Checkpoint
        // Tujuan: Memastikan Checkpoint otomatis terpanggil setelah 10 Commit.
        [Fact]
        public void Test4_PeriodicCheckpoint_ShouldTriggerAfter10Commits()
        {
            _output.WriteLine("SQL: Auto-Checkpoint Trigger");
            var frm = new FailureRecoveryManager(null, new MockStorageManager());

            // Loop 10 kali Commit
            _output.WriteLine("[Action] Simulating 10 Consecutive Commits...");
            for (int i = 0; i < 10; i++)
            {
                var commitEntry = LogEntry.CreateCommit(frm.GetCurrentLSN(), i);
                frm.WriteLogEntry(commitEntry);
            }
            _output.WriteLine("[SUCCESS] 10 Commits executed");

            // Assert
            string content = File.ReadAllText(_logFile);

            // Harus ada kata "CHECKPOINT" di dalam log file
            Assert.Contains("CHECKPOINT", content);
            _output.WriteLine("[SUCCESS] 'CHECKPOINT' entry found in log file");
            
            _output.WriteLine("SUCCESS");
        }

        // TEST 5: Recovery / UNDO (CRITICAL TEST)
        // Tujuan: Memastikan logika 'Rollback' memanggil Query Processor dengan query kebalikannya.
        // Skenario: Kita punya log INSERT, pas di Undo harusnya jadi DELETE.
        [Fact]
        public void Test5_UndoTransaction_ShouldGenerateDeleteQuery()
        {
            _output.WriteLine("SQL: UndoTransaction(50) [Scenario: Undo INSERT]");
            
            var mockQP = new MockQueryProcessor();
            var frm = new FailureRecoveryManager(mockQP, new MockStorageManager());
            var log = new ExecutionLog
            {
                TransactionId = 50,
                Operation = ExecutionLog.OperationType.INSERT,
                TableName = "Barang",
                AfterImage = new Row 
                { 
                    Columns = { { "ID", 123 } }
                }
            };
            _output.WriteLine("[Setup] Log INSERT prepared for T50");

            // Tulis log dan FLUSH
            frm.WriteLog(log);
            frm.FlushLogBuffer(); 
            _output.WriteLine("[Action] Log flushed to disk");
            
            bool result = frm.UndoTransaction(50);
            _output.WriteLine("[Action] UndoTransaction(50) called");

            // Assert
            Assert.True(result, "Undo harus return true");
            _output.WriteLine("[SUCCESS] Undo operation returned true");

            // Cek apakah MockQP menerima perintah DELETE
            Assert.Contains("DELETE FROM Barang", mockQP.LastExecutedQuery);
            _output.WriteLine($"[SUCCESS] Generated Query Verification: {mockQP.LastExecutedQuery}");
            
            Assert.Contains("ID", mockQP.LastExecutedQuery);
            Assert.Contains("123", mockQP.LastExecutedQuery);
            _output.WriteLine("[SUCCESS] Query targets correct RowIdentifier (ID=123)");
            
            _output.WriteLine("SUCCESS");
        }
    }
}