using mDBMS.Common.Data;
using mDBMS.Common.Transaction;

namespace mDBMS.FailureRecovery.Tests
{
    /// <summary>
    /// Test untuk transaction control operations (BEGIN/COMMIT/ABORT)
    /// Menggunakan factory methods + WriteLogEntry
    /// </summary>
    public static class FRM_TransactionControl_Test
    {
        private static readonly string TestLogPath = Path.Combine(Directory.GetCurrentDirectory(), "logs");

        public static void RunAllTests()
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  FRM TRANSACTION CONTROL TESTS");
            Console.WriteLine("===========================================\n");

            int passed = 0;
            int failed = 0;

            // Test 1: BEGIN transaction
            if (Test_BeginTransaction())
            {
                Console.WriteLine("[SUCCESS] Test 1: BEGIN transaction\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 1: BEGIN transaction\n");
                failed++;
            }

            // Test 2: COMMIT transaction
            if (Test_CommitTransaction())
            {
                Console.WriteLine("[SUCCESS] Test 2: COMMIT transaction\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 2: COMMIT transaction\n");
                failed++;
            }

            // Test 3: ABORT transaction
            if (Test_AbortTransaction())
            {
                Console.WriteLine("[SUCCESS] Test 3: ABORT transaction\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 3: ABORT transaction\n");
                failed++;
            }

            // Test 4: Full transaction flow (BEGIN -> INSERT -> COMMIT)
            if (Test_FullTransactionFlow())
            {
                Console.WriteLine("[SUCCESS] Test 4: Full transaction flow\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 4: Full transaction flow\n");
                failed++;
            }

            // Test 5: Periodic checkpoint (10 commits trigger checkpoint)
            if (Test_PeriodicCheckpoint())
            {
                Console.WriteLine("[SUCCESS] Test 5: Periodic checkpoint\n");
                passed++;
            }
            else
            {
                Console.WriteLine("[FAILED] Test 5: Periodic checkpoint\n");
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
                Console.WriteLine("All transaction control tests passed!");
            }
            else
            {
                Console.WriteLine($"WARNING: {failed} test(s) failed. Please review.");
            }
        }

        /// <summary>
        /// Test 1: BEGIN transaction using factory method
        /// </summary>
        private static bool Test_BeginTransaction()
        {
            try
            {
                Console.WriteLine("[Test 1] Testing BEGIN transaction...");

                CleanupLogFile();

                var frm = new FailureRecoveryManager();
                int txnId = 100;

                // Create BEGIN entry using factory method
                var beginEntry = LogEntry.CreateBeginTransaction(frm.GetCurrentLSN(), txnId);

                Console.WriteLine($"  -> Writing BEGIN entry for T{txnId}");
                frm.WriteLogEntry(beginEntry);

                // Manually flush (BEGIN doesn't auto-flush)
                frm.FlushLogBuffer();

                // Verify log file
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                if (!File.Exists(logFilePath))
                {
                    Console.WriteLine("  [X] Log file not created!");
                    return false;
                }

                var logLines = File.ReadAllLines(logFilePath);
                if (logLines.Length == 0)
                {
                    Console.WriteLine("  [X] Log file is empty!");
                    return false;
                }

                // Parse last line
                string lastLine = logLines[^1];
                string[] fields = lastLine.Split('|');

                Console.WriteLine($"  -> Log entry: {lastLine.Substring(0, Math.Min(80, lastLine.Length))}...");
                Console.WriteLine($"  -> Transaction ID: {fields[2]}");
                Console.WriteLine($"  -> Operation: {fields[3]}");

                if (fields[3] != "BEGIN_TRANSACTION" || fields[2] != txnId.ToString())
                {
                    Console.WriteLine("  [X] Log entry mismatch!");
                    return false;
                }

                Console.WriteLine("  [OK] BEGIN transaction logged correctly");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test 2: COMMIT transaction using factory method
        /// </summary>
        private static bool Test_CommitTransaction()
        {
            try
            {
                Console.WriteLine("[Test 2] Testing COMMIT transaction...");

                CleanupLogFile();

                var frm = new FailureRecoveryManager();
                int txnId = 200;

                // Create COMMIT entry using factory method
                var commitEntry = LogEntry.CreateCommit(frm.GetCurrentLSN(), txnId);

                Console.WriteLine($"  -> Writing COMMIT entry for T{txnId}");
                frm.WriteLogEntry(commitEntry);

                // Verify log
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                var logLines = File.ReadAllLines(logFilePath);
                string lastLine = logLines[^1];
                string[] fields = lastLine.Split('|');

                Console.WriteLine($"  -> Log entry: {lastLine.Substring(0, Math.Min(80, lastLine.Length))}...");

                if (fields[3] != "COMMIT" || fields[2] != txnId.ToString())
                {
                    Console.WriteLine("  [X] Log entry mismatch!");
                    return false;
                }

                Console.WriteLine("  [OK] COMMIT transaction logged correctly");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test 3: ABORT transaction using factory method
        /// </summary>
        private static bool Test_AbortTransaction()
        {
            try
            {
                Console.WriteLine("[Test 3] Testing ABORT transaction...");

                CleanupLogFile();

                var frm = new FailureRecoveryManager();
                int txnId = 300;

                // Create ABORT entry using factory method
                var abortEntry = LogEntry.CreateAbort(frm.GetCurrentLSN(), txnId);

                Console.WriteLine($"  -> Writing ABORT entry for T{txnId}");
                frm.WriteLogEntry(abortEntry);

                // Verify log
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                var logLines = File.ReadAllLines(logFilePath);
                string lastLine = logLines[^1];
                string[] fields = lastLine.Split('|');

                Console.WriteLine($"  -> Log entry: {lastLine.Substring(0, Math.Min(80, lastLine.Length))}...");

                if (fields[3] != "ABORT" || fields[2] != txnId.ToString())
                {
                    Console.WriteLine("  [X] Log entry mismatch!");
                    return false;
                }

                Console.WriteLine("  [OK] ABORT transaction logged correctly");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Test 4: Full transaction flow (BEGIN -> INSERT -> COMMIT)
        /// Demonstrates mixing WriteLogEntry (control) and WriteLog (data)
        /// </summary>
        private static bool Test_FullTransactionFlow()
        {
            try
            {
                Console.WriteLine("[Test 4] Testing full transaction flow (BEGIN -> INSERT -> COMMIT)...");

                CleanupLogFile();

                var frm = new FailureRecoveryManager();
                int txnId = 400;

                // Step 1: BEGIN
                Console.WriteLine($"  [Step 1] Writing BEGIN for T{txnId}...");
                var beginEntry = LogEntry.CreateBeginTransaction(frm.GetCurrentLSN(), txnId);
                frm.WriteLogEntry(beginEntry);

                // Step 2: INSERT (using WriteLog from QP)
                Console.WriteLine($"  [Step 2] Writing INSERT for T{txnId}...");
                var insertLog = new ExecutionLog
                {
                    Operation = ExecutionLog.OperationType.INSERT,
                    TransactionId = txnId,
                    TableName = "users",
                    RowIdentifier = "id = 1",
                    BeforeImage = null,
                    AfterImage = new Row
                    {
                        id = "1",
                        Columns = new System.Collections.Generic.Dictionary<string, object>
                        {
                            ["id"] = 1,
                            ["name"] = "Alice"
                        }
                    }
                };
                frm.WriteLog(insertLog);

                // Step 3: COMMIT
                Console.WriteLine($"  [Step 3] Writing COMMIT for T{txnId}...");
                var commitEntry = LogEntry.CreateCommit(frm.GetCurrentLSN(), txnId);
                frm.WriteLogEntry(commitEntry);

                // Verify log sequence
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                var logLines = File.ReadAllLines(logFilePath);

                Console.WriteLine($"  -> Total log entries: {logLines.Length}");

                if (logLines.Length != 3)
                {
                    Console.WriteLine($"  [X] Expected 3 log entries, got {logLines.Length}");
                    return false;
                }

                // Check sequence
                string[] line1 = logLines[0].Split('|');
                string[] line2 = logLines[1].Split('|');
                string[] line3 = logLines[2].Split('|');

                Console.WriteLine($"  -> Entry 1: {line1[3]} (T{line1[2]})");
                Console.WriteLine($"  -> Entry 2: {line2[3]} (T{line2[2]})");
                Console.WriteLine($"  -> Entry 3: {line3[3]} (T{line3[2]})");

                if (line1[3] != "BEGIN_TRANSACTION" || line2[3] != "INSERT" || line3[3] != "COMMIT")
                {
                    Console.WriteLine("  [X] Log sequence incorrect!");
                    return false;
                }

                if (line1[2] != txnId.ToString() || line2[2] != txnId.ToString() || line3[2] != txnId.ToString())
                {
                    Console.WriteLine("  [X] Transaction IDs mismatch!");
                    return false;
                }

                Console.WriteLine("  [OK] Full transaction flow logged correctly");
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
        /// Test 5: Periodic checkpoint (10 commits trigger checkpoint automatically)
        /// </summary>
        private static bool Test_PeriodicCheckpoint()
        {
            try
            {
                Console.WriteLine("[Test 5] Testing periodic checkpoint (10 commits trigger checkpoint)...");

                CleanupLogFile();

                var frm = new FailureRecoveryManager();

                // Commit 10 transactions to trigger periodic checkpoint
                Console.WriteLine($"  -> Committing 10 transactions...");
                for (int i = 1; i <= 10; i++)
                {
                    // BEGIN
                    var beginEntry = LogEntry.CreateBeginTransaction(frm.GetCurrentLSN(), i);
                    frm.WriteLogEntry(beginEntry);

                    // COMMIT (should trigger checkpoint on 10th commit)
                    var commitEntry = LogEntry.CreateCommit(frm.GetCurrentLSN(), i);
                    frm.WriteLogEntry(commitEntry);

                    if (i == 10)
                    {
                        Console.WriteLine($"  -> 10th commit reached, checkpoint should trigger automatically");
                    }
                }

                // Verify log contains CHECKPOINT entry
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                var logLines = File.ReadAllLines(logFilePath);

                Console.WriteLine($"  -> Total log entries: {logLines.Length}");

                // Should have: 10 BEGIN + 10 COMMIT + 1 CHECKPOINT = 21 entries
                if (logLines.Length < 21)
                {
                    Console.WriteLine($"  [X] Expected at least 21 log entries (10 BEGIN + 10 COMMIT + 1 CHECKPOINT), got {logLines.Length}");
                    return false;
                }

                // Check for CHECKPOINT entry
                bool foundCheckpoint = false;
                foreach (var line in logLines)
                {
                    string[] fields = line.Split('|');
                    if (fields.Length >= 4 && fields[3] == "CHECKPOINT")
                    {
                        foundCheckpoint = true;
                        Console.WriteLine($"  -> Found CHECKPOINT entry at LSN {fields[0]}");
                        break;
                    }
                }

                if (!foundCheckpoint)
                {
                    Console.WriteLine("  [X] CHECKPOINT entry not found in log!");
                    return false;
                }

                Console.WriteLine("  [OK] Periodic checkpoint triggered correctly after 10 commits");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [X] Exception: {ex.Message}");
                Console.WriteLine($"  Stack: {ex.StackTrace}");
                return false;
            }
        }

        // ==================== Helper Methods ====================

        private static void CleanupLogFile()
        {
            try
            {
                string logFilePath = Path.Combine(TestLogPath, "mDBMS.log");
                if (File.Exists(logFilePath))
                {
                    File.Delete(logFilePath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not cleanup log file - {ex.Message}");
            }
        }
    }
}
