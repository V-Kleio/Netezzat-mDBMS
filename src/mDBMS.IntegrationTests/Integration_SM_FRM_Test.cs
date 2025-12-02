using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using mDBMS.Common.Data;
using mDBMS.Common.Transaction;
using mDBMS.FailureRecovery;
using mDBMS.StorageManager;

namespace mDBMS.FailureRecovery.Tests
{
    /// <summary>
    /// Integration tests for SM <-> FRM (Buffer and Disk I/O)
    /// Tests the complete flow: QP -> SM -> Buffer -> Checkpoint -> Disk
    /// </summary>
    public static class Integration_SM_FRM_Test
    {
        private static readonly string TestDataPath = AppDomain.CurrentDomain.BaseDirectory;
        private static readonly string TestLogPath = Path.Combine(Directory.GetCurrentDirectory(), "logs");

        public static void RunAllTests()
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  SM <-> FRM INTEGRATION TESTS");
            Console.WriteLine("===========================================\n");

            CleanupTestFiles();

            int passed = 0;
            int failed = 0;

            // Test 1: SM writes to buffer
            if (Test_SM_WritesToBuffer())
            {
                Console.WriteLine("[SUCCESS] Test 1: SM writes to buffer\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 1: SM writes to buffer\n");
                failed++;
            }

            // Test 2: Buffer flush creates .dat file
            if (Test_BufferFlushCreatesDatFile())
            {
                Console.WriteLine("[SUCCESS] Test 2: Buffer flush creates .dat file\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 2: Buffer flush creates .dat file\n");
                failed++;
            }

            // Test 3: Log operations create .log file
            if (Test_LogOperationsCreateLogFile())
            {
                Console.WriteLine("[SUCCESS] Test 3: Log operations create .log file\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 3: Log operations create .log file\n");
                failed++;
            }

            // Test 4: Full flow (Write -> Buffer -> Checkpoint -> Disk)
            if (Test_FullFlow_WriteBufferCheckpointDisk())
            {
                Console.WriteLine("[SUCCESS] Test 4: Full flow (Write->Buffer->Checkpoint->Disk)\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 4: Full flow (Write->Buffer->Checkpoint->Disk)\n");
                failed++;
            }

            // Test 5: Read from buffer (dirty data)
            if (Test_ReadFromBufferBeforeFlush())
            {
                Console.WriteLine("[SUCCESS] Test 5: Read from buffer before flush\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 5: Read from buffer before flush\n");
                failed++;
            }

            // Summary
            Console.WriteLine("===========================================");
            Console.WriteLine($"TOTAL: {passed + failed} tests");
            Console.WriteLine($"[SUCCESS] PASSED: {passed}");
            Console.WriteLine($"[FAILED] FAILED: {failed}");
            Console.WriteLine("===========================================\n");

            if (failed == 0)
            {
                Console.WriteLine("All integration tests passed!");
            }
            else
            {
                Console.WriteLine($"WARNING: {failed} test(s) failed. Please review.");
            }
        }

        /// <summary>
        /// Test 1: Verify SM writes to buffer instead of direct disk
        /// </summary>
        private static bool Test_SM_WritesToBuffer()
        {
            try
            {
                Console.WriteLine("[Test 1] Testing SM writes to buffer...");

                // Setup: Create schema and table file
                var schema = CreateTestSchema();
                string tableName = "test_buffer_write";
                CreateTestTableFile(tableName, schema);

                // Initialize FRM and SM
                var frm = new FailureRecoveryManager();
                var sm = new StorageEngine(frm);

                // Create DataWrite
                var dataWrite = new DataWrite(tableName, new Dictionary<string, object>
                {
                    ["user_id"] = 1,
                    ["name"] = "TestUser",
                    ["age"] = 25
                });

                Console.WriteLine($"  -> Adding row to table '{tableName}'");
                int result = sm.AddBlock(dataWrite);

                if (result != 1)
                {
                    Console.WriteLine($"  [X] AddBlock failed (returned {result})");
                    return false;
                }

                // Check buffer has dirty page
                var dirtyPages = frm.GetDirtyPages();
                Console.WriteLine($"  -> Buffer contains {dirtyPages.Count} dirty page(s)");

                if (dirtyPages.Count == 0)
                {
                    Console.WriteLine("  [X] No dirty pages in buffer!");
                    return false;
                }

                var page = dirtyPages[0];
                Console.WriteLine($"  -> Dirty page: {page.TableName}, Block {page.BlockID}, IsDirty={page.IsDirty}");

                if (page.TableName != tableName || !page.IsDirty)
                {
                    Console.WriteLine("  [X] Page not marked as dirty or wrong table!");
                    return false;
                }

                Console.WriteLine("  [OK] SM successfully wrote to buffer (page is dirty)");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test 2: Verify checkpoint flushes buffer and creates .dat file
        /// </summary>
        private static bool Test_BufferFlushCreatesDatFile()
        {
            try
            {
                Console.WriteLine("[Test 2] Testing buffer flush creates .dat file...");

                var schema = CreateTestSchema();
                string tableName = "test_flush_disk";
                string datFilePath = Path.Combine(TestDataPath, $"{tableName}.dat");

                CreateTestTableFile(tableName, schema);

                var frm = new FailureRecoveryManager(null, new StorageEngine());
                var sm = new StorageEngine(frm);

                // Write data to buffer
                var dataWrite = new DataWrite(tableName, new Dictionary<string, object>
                {
                    ["user_id"] = 100,
                    ["name"] = "FlushTest",
                    ["age"] = 30
                });

                Console.WriteLine($"  -> Adding row to buffer");
                sm.AddBlock(dataWrite);

                // Verify buffer has data
                var dirtyPagesBefore = frm.GetDirtyPages();
                Console.WriteLine($"  -> Dirty pages before checkpoint: {dirtyPagesBefore.Count}");

                if (dirtyPagesBefore.Count == 0)
                {
                    Console.WriteLine("  [X] No data in buffer to flush!");
                    return false;
                }

                // Get file size before checkpoint
                long fileSizeBefore = new FileInfo(datFilePath).Length;
                Console.WriteLine($"  -> .dat file size before flush: {fileSizeBefore} bytes");

                // Perform checkpoint (should flush to disk)
                Console.WriteLine($"  -> Executing SaveCheckpoint()...");
                frm.SaveCheckpoint();

                // Check buffer is cleared
                var dirtyPagesAfter = frm.GetDirtyPages();
                Console.WriteLine($"  -> Dirty pages after checkpoint: {dirtyPagesAfter.Count}");

                // Check file size increased
                long fileSizeAfter = new FileInfo(datFilePath).Length;
                Console.WriteLine($"  -> .dat file size after flush: {fileSizeAfter} bytes");

                if (fileSizeAfter <= fileSizeBefore)
                {
                    Console.WriteLine($"  [X] File size did not increase! (Before: {fileSizeBefore}, After: {fileSizeAfter})");
                    return false;
                }

                Console.WriteLine($"  [OK] Checkpoint successfully flushed buffer to disk (+{fileSizeAfter - fileSizeBefore} bytes)");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                Console.WriteLine($"  Stack: {ex.StackTrace}");
                return false;
            }
        }

        /// <summary>
        /// Test 3: Verify log operations create .log file with correct format
        /// </summary>
        private static bool Test_LogOperationsCreateLogFile()
        {
            try
            {
                Console.WriteLine("[Test 3] Testing log operations create .log file...");

                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");

                // Delete existing log file
                if (File.Exists(logFilePath))
                {
                    File.Delete(logFilePath);
                    Console.WriteLine($"  -> Deleted existing log file");
                }

                // Initialize FRM (creates log directory)
                var frm = new FailureRecoveryManager(null, new StorageEngine());
                var sm = new StorageEngine(frm);
                // Create ExecutionLog entries
                var insertLog = new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.INSERT,
                    TransactionId = 1,
                    TableName = "users",
                    RowIdentifier = "id = 1",
                    BeforeImage = null,
                    AfterImage = new Row
                    {
                        id = "1",
                        Columns = new Dictionary<string, object>
                        {
                            ["user_id"] = 1,
                            ["name"] = "John",
                            ["age"] = 25
                        }
                    }
                };

                Console.WriteLine($"  -> Writing INSERT log entry (TxnID=1)");
                frm.WriteLog(insertLog);

                // Check log file exists
                if (!File.Exists(logFilePath))
                {
                    Console.WriteLine($"  [X] Log file not created at {logFilePath}");
                    return false;
                }

                Console.WriteLine($"  [OK] Log file created: {logFilePath}");

                // Read and verify log content
                var logLines = File.ReadAllLines(logFilePath);
                Console.WriteLine($"  -> Log file contains {logLines.Length} line(s)");

                if (logLines.Length == 0)
                {
                    Console.WriteLine("  [X] Log file is empty!");
                    return false;
                }

                // Verify log format (LSN|Timestamp|TxnId|OpType|Table|RowId|BeforeImage|AfterImage)
                string firstLine = logLines[^1]; // Get last line (most recent)
                Console.WriteLine($"  -> Latest log entry: {firstLine.Substring(0, Math.Min(80, firstLine.Length))}...");

                string[] fields = firstLine.Split('|');
                if (fields.Length < 8)
                {
                    Console.WriteLine($"  [X] Invalid log format (expected 8 fields, got {fields.Length})");
                    return false;
                }

                // Verify fields
                Console.WriteLine($"  -> LSN: {fields[0]}");
                Console.WriteLine($"  -> Transaction ID: {fields[2]}");
                Console.WriteLine($"  -> Operation: {fields[3]}");
                Console.WriteLine($"  -> Table: {fields[4]}");

                if (fields[3] != "INSERT" || fields[4] != "users")
                {
                    Console.WriteLine("  [X] Log content mismatch!");
                    return false;
                }

                Console.WriteLine("  [OK] Log file format is correct");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test 4: Full integration flow (Write -> Buffer -> Checkpoint -> Disk -> Read)
        /// </summary>
        private static bool Test_FullFlow_WriteBufferCheckpointDisk()
        {
            try
            {
                Console.WriteLine("[Test 4] Testing full flow: Write -> Buffer -> Checkpoint -> Disk -> Read...");

                var schema = CreateTestSchema();
                string tableName = "test_full_flow";
                CreateTestTableFile(tableName, schema);

                var frm = new FailureRecoveryManager(null, new StorageEngine());
                var sm = new StorageEngine(frm);

                // Step 1: Write data
                Console.WriteLine("  [Step 1] Writing data to buffer...");
                var dataWrite = new DataWrite(tableName, new Dictionary<string, object>
                {
                    ["user_id"] = 999,
                    ["name"] = "FullFlowTest",
                    ["age"] = 40
                });

                sm.AddBlock(dataWrite);
                Console.WriteLine("  [OK] Data written to buffer");

                // Step 2: Verify buffer has dirty page
                Console.WriteLine("  [Step 2] Verifying buffer state...");
                var dirtyPages = frm.GetDirtyPages();
                if (dirtyPages.Count == 0)
                {
                    Console.WriteLine("  [X] Buffer is empty!");
                    return false;
                }
                Console.WriteLine($"  [OK] Buffer contains {dirtyPages.Count} dirty page(s)");

                // Step 3: Checkpoint (flush to disk)
                Console.WriteLine("  [Step 3] Executing checkpoint...");
                frm.SaveCheckpoint();
                Console.WriteLine("  [OK] Checkpoint completed");

                // Step 4: Verify buffer is cleared
                Console.WriteLine("  [Step 4] Verifying buffer is cleared...");
                var dirtyPagesAfter = frm.GetDirtyPages();
                if (dirtyPagesAfter.Count > 0)
                {
                    Console.WriteLine($"  [X] Buffer still has {dirtyPagesAfter.Count} dirty pages!");
                    return false;
                }
                Console.WriteLine("  [OK] Buffer is clear");

                // Step 5: Read data back from disk
                Console.WriteLine("  [Step 5] Reading data from disk...");
                var dataRetrieval = new DataRetrieval(tableName, new[] { "id", "name", "age" });
                var rows = sm.ReadBlock(dataRetrieval);

                int rowCount = 0;
                foreach (var row in rows)
                {
                    rowCount++;
                    Console.WriteLine($"  -> Row {rowCount}: user_id={row["user_id"]}, name={row["name"]}, age={row["age"]}");
                }

                if (rowCount == 0)
                {
                    Console.WriteLine("  [X] No data found on disk!");
                    return false;
                }

                Console.WriteLine($"  [OK] Successfully read {rowCount} row(s) from disk");
                Console.WriteLine("  [OK] Full flow completed successfully!");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                Console.WriteLine($"  Stack: {ex.StackTrace}");
                return false;
            }
        }

        /// <summary>
        /// Test 5: Verify reading from buffer returns dirty (uncommitted) data
        /// </summary>
        private static bool Test_ReadFromBufferBeforeFlush()
        {
            try
            {
                Console.WriteLine("[Test 5] Testing read from buffer (dirty data)...");

                var schema = CreateTestSchema();
                string tableName = "test_buffer_read";
                CreateTestTableFile(tableName, schema);

                var frm = new FailureRecoveryManager(null, new StorageEngine());
                var sm = new StorageEngine(frm);

                // Write initial data and flush to disk
                Console.WriteLine("  -> Writing initial data and flushing to disk...");
                var initialWrite = new DataWrite(tableName, new Dictionary<string, object>
                {
                    ["user_id"] = 1,
                    ["name"] = "InitialData",
                    ["age"] = 20
                });
                sm.AddBlock(initialWrite);
                frm.SaveCheckpoint();

                // Write new data to buffer (not flushed)
                Console.WriteLine("  -> Writing new data to buffer (not flushed)...");
                var newWrite = new DataWrite(tableName, new Dictionary<string, object>
                {
                    ["user_id"] = 2,
                    ["name"] = "BufferData",
                    ["age"] = 30
                });
                sm.AddBlock(newWrite);

                // Verify buffer has dirty page
                var dirtyPages = frm.GetDirtyPages();
                Console.WriteLine($"  -> Buffer has {dirtyPages.Count} dirty page(s)");

                if (dirtyPages.Count == 0)
                {
                    Console.WriteLine("  [X] No dirty pages in buffer!");
                    return false;
                }

                // Read data (should include buffer data)
                Console.WriteLine("  -> Reading data (should include buffer data)...");
                var dataRetrieval = new DataRetrieval(tableName, new[] { "id", "name", "age" });
                var rows = sm.ReadBlock(dataRetrieval);

                int rowCount = 0;
                bool foundBufferData = false;

                foreach (var row in rows)
                {
                    rowCount++;
                    Console.WriteLine($"  -> Row {rowCount}: user_id={row["user_id"]}, name={row["name"]}");

                    if (row["name"].ToString() == "BufferData")
                    {
                        foundBufferData = true;
                    }
                }

                if (!foundBufferData)
                {
                    Console.WriteLine("  [X] Buffer data not found in read results!");
                    return false;
                }

                Console.WriteLine("  [OK] Successfully read dirty data from buffer");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        // ==================== Helper Methods ====================

        private static TableSchema CreateTestSchema()
        {
            return new TableSchema
            {
                TableName = "test_table",
                Columns = new List<ColumnSchema>
                {
                    new ColumnSchema { Name = "user_id", Type = DataType.Int },      // ← Changed from "id"
                    new ColumnSchema { Name = "name", Type = DataType.String, Length = 50 },
                    new ColumnSchema { Name = "age", Type = DataType.Int }
                }
            };
        }

        private static void CreateTestTableFile(string tableName, TableSchema schema)
        {
            string filePath = Path.Combine(TestDataPath, $"{tableName}.dat");

            // Create file with schema header
            using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write))
            {
                SchemaSerializer.WriteSchema(filePath, schema); return;
            }

            Console.WriteLine($"  -> Created test table file: {filePath}");
        }

        private static void CleanupTestFiles()
        {
            try
            {
                // Delete test .dat files
                string[] testFiles = {
                    "test_buffer_write.dat",
                    "test_flush_disk.dat",
                    "test_full_flow.dat",
                    "test_buffer_read.dat"
                };

                foreach (var file in testFiles)
                {
                    string path = Path.Combine(TestDataPath, file);
                    if (File.Exists(path))
                    {
                        File.Delete(path);
                    }
                }

                Console.WriteLine("Cleaned up existing test files\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not cleanup test files - {ex.Message}\n");
            }
        }
    }
}
