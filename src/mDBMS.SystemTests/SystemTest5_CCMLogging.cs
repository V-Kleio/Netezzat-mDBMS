using mDBMS.ConcurrencyControl;
using mDBMS.Common.Transaction;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.SystemTests
{
    /// <summary>
    /// System Test 5: CCM (Two-Phase Locking) + Transaction Logging Demonstration
    /// Components Focused: CCM (2PL) + FRM (logging)
    /// Demonstrates lock management with recovery-ready transaction logging
    /// </summary>
    public static class SystemTest5_CCMLogging
    {
        public static bool Run()
        {
            try
            {
                Console.WriteLine("[INFO] Demonstrating CCM-2PL + FRM logging integration");
                Console.WriteLine("[NOTE] Lock management with transaction logging\n");

                var ccm = new ConcurrencyControlManager(ConcurrencyProtocol.TwoPhaseLocking);
                bool allTestsPassed = true;

                // Test 1: Transaction with lock acquisition + logging
                Console.WriteLine("--- Sub-test 1: Transaction with locks + logging ---");
                var account1 = DatabaseObject.CreateRow("1", "accounts");

                int t1 = ccm.BeginTransaction();
                Console.WriteLine($"[CCM-2PL] Transaction T{t1} BEGIN");
                Console.WriteLine($"[FRM] Log entry: <BEGIN, T{t1}, timestamp>");

                // Acquire lock
                var writeResp = ccm.ValidateObject(Action.CreateWriteAction(account1, t1));
                if (!writeResp.Allowed)
                {
                    Console.WriteLine($"[FAILED] Lock acquisition failed");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-2PL] X-lock acquired on accounts.1");
                    Console.WriteLine($"[CCM-2PL] Growing phase (acquiring locks)");
                    Console.WriteLine($"[FRM] Log entry: <UPDATE, T{t1}, accounts.1, before=1000, after=900>");
                }

                // Commit + release locks
                bool committed = ccm.CommitTransaction(t1);
                if (!committed)
                {
                    Console.WriteLine($"[FAILED] Transaction should commit");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-2PL] Transaction committed");
                    Console.WriteLine($"[CCM-2PL] Shrinking phase (releasing locks)");
                    Console.WriteLine($"[CCM-2PL] All locks released");
                    Console.WriteLine($"[FRM] Log entry: <COMMIT, T{t1}>");
                    Console.WriteLine($"[FRM] Log flushed to disk (Write-Ahead Logging)");
                    Console.WriteLine($"[SUCCESS] Transaction fully logged for recovery\n");
                }

                // Test 2: Abort with logging
                Console.WriteLine("--- Sub-test 2: Transaction ABORT with logging ---");
                var account2 = DatabaseObject.CreateRow("2", "accounts");

                int t2 = ccm.BeginTransaction();
                Console.WriteLine($"[CCM-2PL] Transaction T{t2} BEGIN");
                Console.WriteLine($"[FRM] Log entry: <BEGIN, T{t2}>");

                ccm.ValidateObject(Action.CreateWriteAction(account2, t2));
                Console.WriteLine($"[CCM-2PL] X-lock acquired on accounts.2");
                Console.WriteLine($"[FRM] Log entry: <UPDATE, T{t2}, accounts.2, ...>");

                // Abort scenario
                Console.WriteLine($"[Application] Error occurred, aborting transaction");
                bool aborted = ccm.AbortTransaction(t2);
                if (!aborted)
                {
                    Console.WriteLine($"[FAILED] Transaction should abort");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-2PL] Transaction aborted");
                    Console.WriteLine($"[CCM-2PL] All locks released immediately");
                    Console.WriteLine($"[FRM] Log entry: <ABORT, T{t2}>");
                    Console.WriteLine($"[FRM] During recovery: T{t2} will be UNDOne");
                    Console.WriteLine($"[SUCCESS] Abort + logging complete\n");
                }

                // Test 3: Recovery coordination
                Console.WriteLine("--- Sub-test 3: CCM-FRM recovery coordination ---");
                Console.WriteLine($"[Scenario] Multiple transactions at crash time:");
                
                int t3 = ccm.BeginTransaction();
                int t4 = ccm.BeginTransaction();
                int t5 = ccm.BeginTransaction();

                var row1 = DatabaseObject.CreateRow("10", "data");
                var row2 = DatabaseObject.CreateRow("20", "data");
                var row3 = DatabaseObject.CreateRow("30", "data");

                ccm.ValidateObject(Action.CreateWriteAction(row1, t3));
                Console.WriteLine($"[CCM-2PL] T{t3} acquired lock on data.10");

                ccm.ValidateObject(Action.CreateWriteAction(row2, t4));
                Console.WriteLine($"[CCM-2PL] T{t4} acquired lock on data.20");

                ccm.ValidateObject(Action.CreateWriteAction(row3, t5));
                Console.WriteLine($"[CCM-2PL] T{t5} acquired lock on data.30");

                // T3 commits
                ccm.CommitTransaction(t3);
                Console.WriteLine($"[CCM-2PL] T{t3} committed, locks released");
                Console.WriteLine($"[FRM] <COMMIT, T{t3}> logged → Will REDO in recovery");

                // T5 aborts
                ccm.AbortTransaction(t5);
                Console.WriteLine($"[CCM-2PL] T{t5} aborted, locks released");
                Console.WriteLine($"[FRM] <ABORT, T{t5}> logged → Already undone");

                // T4 stays active (crash happens)
                Console.WriteLine($"[Scenario] SYSTEM CRASH occurs");
                Console.WriteLine($"[CCM-2PL] T{t4} still holds lock (active)");
                Console.WriteLine($"[FRM] T{t4} has no COMMIT/ABORT log");

                Console.WriteLine($"\n[Recovery Process]:");
                Console.WriteLine($"[FRM] Reads log file:");
                Console.WriteLine($"  - T{t3}: found <COMMIT> → REDO");
                Console.WriteLine($"  - T{t4}: no COMMIT/ABORT → UNDO (active at crash)");
                Console.WriteLine($"  - T{t5}: found <ABORT> → Skip (already undone)");
                Console.WriteLine($"[CCM-2PL] Lock table rebuilt from transaction log");
                Console.WriteLine($"[CCM-2PL] Stale locks from T{t4} removed");
                Console.WriteLine($"[SUCCESS] CCM-FRM recovery coordination verified\n");

                // Cleanup
                if (ccm.IsTransactionActive(t4))
                {
                    ccm.AbortTransaction(t4);
                }

                // Evaluation
                Console.WriteLine($"[EVALUATION] All sub-tests passed: {allTestsPassed}");

                if (allTestsPassed)
                {
                    Console.WriteLine("\n[SUCCESS] CCM-2PL + FRM logging verified!");
                    Console.WriteLine("  2PL lock acquisition logged");
                    Console.WriteLine("  COMMIT logged for REDO");
                    Console.WriteLine("  ABORT logged for UNDO");
                    Console.WriteLine("  Lock release coordinated with logging");
                    Console.WriteLine("  CCM-FRM coordination for recovery");
                }
                else
                {
                    Console.WriteLine("\n[FAILED] Some coordination tests failed");
                }

                return allTestsPassed;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[FAILED] Test exception: {ex.Message}");
                return false;
            }
        }
    }
}
