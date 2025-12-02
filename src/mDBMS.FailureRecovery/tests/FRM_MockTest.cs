using mDBMS.Common.Transaction;
using mDBMS.Common.Data;

namespace mDBMS.FailureRecovery
{
    /// <summary>
    /// Mock test untuk FRM - bisa dijalankan langsung tanpa dependency
    /// Usage: FRM_MockTest.RunAllTests();
    /// </summary>
    public static class FRM_MockTest
    {
        public static void RunAllTests()
        {
            Console.WriteLine("========================================");
            Console.WriteLine("   FRM MOCK TESTS");
            Console.WriteLine("========================================\n");

            int passCount = 0;
            int failCount = 0;

            // Test 1: WriteLog with INSERT operation
            if (Test_WriteLog_Insert())
            {
                Console.WriteLine("✅ Test_WriteLog_Insert PASSED");
                passCount++;
            }
            else
            {
                Console.WriteLine("❌ Test_WriteLog_Insert FAILED");
                failCount++;
            }

            // Test 2: WriteLog with UPDATE operation
            if (Test_WriteLog_Update())
            {
                Console.WriteLine("✅ Test_WriteLog_Update PASSED");
                passCount++;
            }
            else
            {
                Console.WriteLine("❌ Test_WriteLog_Update FAILED");
                failCount++;
            }

            // Test 3: WriteLog with DELETE operation
            if (Test_WriteLog_Delete())
            {
                Console.WriteLine("✅ Test_WriteLog_Delete PASSED");
                passCount++;
            }
            else
            {
                Console.WriteLine("❌ Test_WriteLog_Delete FAILED");
                failCount++;
            }

            // Test 4: Buffer operations
            if (Test_BufferOperations())
            {
                Console.WriteLine("✅ Test_BufferOperations PASSED");
                passCount++;
            }
            else
            {
                Console.WriteLine("❌ Test_BufferOperations FAILED");
                failCount++;
            }

            // Test 5: LSN increment
            if (Test_LSN_Increment())
            {
                Console.WriteLine("✅ Test_LSN_Increment PASSED");
                passCount++;
            }
            else
            {
                Console.WriteLine("❌ Test_LSN_Increment FAILED");
                failCount++;
            }

            Console.WriteLine("\n========================================");
            Console.WriteLine($"TOTAL: {passCount} PASSED, {failCount} FAILED");
            Console.WriteLine("========================================\n");
        }

        /// <summary>
        /// Test WriteLog dengan INSERT operation
        /// </summary>
        private static bool Test_WriteLog_Insert()
        {
            try
            {
                var frm = new FailureRecoveryManager();

                var executionLog = new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.INSERT,
                    TransactionId = 1,
                    TableName = "Users",
                    RowIdentifier = "id=1",
                    BeforeImage = null, // INSERT tidak punya before image
                    AfterImage = new Row
                    {
                        Columns = new Dictionary<string, object?>
                        {
                            { "id", 1 },
                            { "name", "John Doe" },
                            { "email", "john@example.com" }
                        }
                    }
                };

                // Execute WriteLog
                frm.WriteLog(executionLog);

                // Verify: Cek apakah log file tertulis
                string logPath = Path.Combine(Directory.GetCurrentDirectory(), "logs", "mDBMS.log");
                if (!File.Exists(logPath))
                {
                    Console.WriteLine("  [FAIL] Log file tidak tercipta");
                    return false;
                }

                // Read last line dari log file
                var lines = File.ReadAllLines(logPath);
                if (lines.Length == 0)
                {
                    Console.WriteLine("  [FAIL] Log file kosong");
                    return false;
                }

                string lastLine = lines[^1];

                // Verify: Cek apakah ada "INSERT" dan "Users" di log
                if (!lastLine.Contains("INSERT") || !lastLine.Contains("Users"))
                {
                    Console.WriteLine($"  [FAIL] Log entry tidak sesuai: {lastLine}");
                    return false;
                }

                Console.WriteLine($"  [INFO] Log entry: {lastLine}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test WriteLog dengan UPDATE operation
        /// </summary>
        private static bool Test_WriteLog_Update()
        {
            try
            {
                var frm = new FailureRecoveryManager();

                var beforeRow = new Row
                {
                    Columns = new Dictionary<string, object?>
                    {
                        { "id", 1 },
                        { "name", "John Doe" },
                        { "email", "john@example.com" }
                    }
                };

                var afterRow = new Row
                {
                    Columns = new Dictionary<string, object?>
                    {
                        { "id", 1 },
                        { "name", "Jane Doe" },
                        { "email", "jane@example.com" }
                    }
                };

                var executionLog = new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.UPDATE,
                    TransactionId = 2,
                    TableName = "Users",
                    RowIdentifier = "id=1",
                    BeforeImage = beforeRow,
                    AfterImage = afterRow
                };

                // Execute WriteLog
                frm.WriteLog(executionLog);

                // Verify: Cek log file
                string logPath = Path.Combine(Directory.GetCurrentDirectory(), "logs", "mDBMS.log");
                var lines = File.ReadAllLines(logPath);
                string lastLine = lines[^1];

                if (!lastLine.Contains("UPDATE") || !lastLine.Contains("Users"))
                {
                    Console.WriteLine($"  [FAIL] Log entry tidak sesuai: {lastLine}");
                    return false;
                }

                Console.WriteLine($"  [INFO] Log entry: {lastLine}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test WriteLog dengan DELETE operation
        /// </summary>
        private static bool Test_WriteLog_Delete()
        {
            try
            {
                var frm = new FailureRecoveryManager();

                var beforeRow = new Row
                {
                    Columns = new Dictionary<string, object?>
                    {
                        { "id", 1 },
                        { "name", "John Doe" },
                        { "email", "john@example.com" }
                    }
                };

                var executionLog = new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.DELETE,
                    TransactionId = 3,
                    TableName = "Users",
                    RowIdentifier = "id=1",
                    BeforeImage = beforeRow,
                    AfterImage = null // DELETE tidak punya after image
                };

                // Execute WriteLog
                frm.WriteLog(executionLog);

                // Verify
                string logPath = Path.Combine(Directory.GetCurrentDirectory(), "logs", "mDBMS.log");
                var lines = File.ReadAllLines(logPath);
                string lastLine = lines[^1];

                if (!lastLine.Contains("DELETE") || !lastLine.Contains("Users"))
                {
                    Console.WriteLine($"  [FAIL] Log entry tidak sesuai: {lastLine}");
                    return false;
                }

                Console.WriteLine($"  [INFO] Log entry: {lastLine}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test Buffer operations (ReadFromBuffer, WriteToBuffer)
        /// </summary>
        private static bool Test_BufferOperations()
        {
            try
            {
                var frm = new FailureRecoveryManager();

                // Create test page
                var testPage = new Page("TestTable", 1, new byte[4096], true);

                // Fill dengan test data
                for (int i = 0; i < 100; i++)
                {
                    testPage.Data[i] = (byte)i;
                }

                // Test 1: WriteToBuffer
                frm.WriteToBuffer(testPage);
                Console.WriteLine("  [INFO] Page written to buffer");

                // Test 2: ReadFromBuffer
                byte[] readData = frm.ReadFromBuffer("TestTable", 1);

                if (readData.Length == 0)
                {
                    Console.WriteLine("  [FAIL] ReadFromBuffer returned empty data");
                    return false;
                }

                // Verify data integrity
                for (int i = 0; i < 100; i++)
                {
                    if (readData[i] != (byte)i)
                    {
                        Console.WriteLine($"  [FAIL] Data mismatch at index {i}");
                        return false;
                    }
                }

                Console.WriteLine("  [INFO] Buffer read/write verified successfully");

                // Test 3: Read non-existent page
                byte[] emptyData = frm.ReadFromBuffer("NonExistent", 999);
                if (emptyData.Length != 0)
                {
                    Console.WriteLine("  [FAIL] Non-existent page should return empty array");
                    return false;
                }

                Console.WriteLine("  [INFO] Non-existent page returns empty array correctly");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test LSN increment setiap WriteLog
        /// </summary>
        private static bool Test_LSN_Increment()
        {
            try
            {
                // Cleanup log file untuk test ini
                string logPath = Path.Combine(Directory.GetCurrentDirectory(), "logs", "mDBMS.log");
                if (File.Exists(logPath))
                {
                    File.Delete(logPath);
                }

                var frm = new FailureRecoveryManager();

                // Write 3 log entries
                for (int i = 0; i < 3; i++)
                {
                    var executionLog = new ExecutionLog
                    {
                        Operation = ExecutionLog.OperationType.INSERT,
                        TransactionId = i + 1,
                        TableName = "TestTable",
                        RowIdentifier = $"id={i}",
                        BeforeImage = null,
                        AfterImage = new Row
                        {
                            Columns = new Dictionary<string, object?> { { "id", i } }
                        }
                    };

                    frm.WriteLog(executionLog);
                }

                // Read all log entries
                var lines = File.ReadAllLines(logPath);

                if (lines.Length != 3)
                {
                    Console.WriteLine($"  [FAIL] Expected 3 log entries, got {lines.Length}");
                    return false;
                }

                // Parse LSN from each line (LSN adalah field pertama, pipe-delimited)
                var lsns = new List<long>();
                foreach (var line in lines)
                {
                    var parts = line.Split('|');
                    if (parts.Length > 0 && long.TryParse(parts[0], out long lsn))
                    {
                        lsns.Add(lsn);
                    }
                }

                // Verify LSN increment
                for (int i = 1; i < lsns.Count; i++)
                {
                    if (lsns[i] != lsns[i - 1] + 1)
                    {
                        Console.WriteLine($"  [FAIL] LSN not incrementing correctly: {lsns[i - 1]} -> {lsns[i]}");
                        return false;
                    }
                }

                Console.WriteLine($"  [INFO] LSN increments correctly: {string.Join(" -> ", lsns)}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Helper: Create mock ExecutionLog for testing
        /// </summary>
        public static ExecutionLog CreateMockExecutionLog(
            ExecutionLog.OperationType operation,
            int transactionId,
            string tableName,
            string rowIdentifier,
            Row? beforeImage = null,
            Row? afterImage = null)
        {
            return new ExecutionLog
            {
                Operation = operation,
                TransactionId = transactionId,
                TableName = tableName,
                RowIdentifier = rowIdentifier,
                BeforeImage = beforeImage,
                AfterImage = afterImage
            };
        }

        /// <summary>
        /// Helper: Create mock Row for testing
        /// </summary>
        public static Row CreateMockRow(Dictionary<string, object?> columns)
        {
            return new Row { Columns = columns };
        }

        /// <summary>
        /// Helper: Clean up test files
        /// </summary>
        public static void CleanupTestFiles()
        {
            try
            {
                string logPath = Path.Combine(Directory.GetCurrentDirectory(), "logs", "mDBMS.log");
                if (File.Exists(logPath))
                {
                    File.Delete(logPath);
                    Console.WriteLine("[CLEANUP] Test log file deleted");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CLEANUP ERROR] {ex.Message}");
            }
        }
    }
}
