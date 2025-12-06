using mDBMS.Common.Transaction;
using mDBMS.ConcurrencyControl;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.ConcurrencyControl.Tests
{
    public static class TimestampCCMTest
    {
        public static void RunAllTests()
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  TIMESTAMP ORDERING PROTOCOL TESTS");
            Console.WriteLine("===========================================\n");

            int passed = 0;
            int failed = 0;

            if (Test_SingleTransaction_Commit()) passed++; else failed++;
            if (Test_ReadOperation_Success()) passed++; else failed++;
            if (Test_ReadOperation_Abort_ObsoleteRead()) passed++; else failed++;
            if (Test_WriteOperation_Abort_TooLate()) passed++; else failed++;
            if (Test_ThomasWriteRule()) passed++; else failed++;
            if (Test_MultipleTransactions_NoConflict()) passed++; else failed++;

            Console.WriteLine("===========================================");
            Console.WriteLine($"TOTAL: {passed + failed} tests");
            Console.WriteLine($"[SUCCESS] PASSED: {passed}");
            Console.WriteLine($"[FAILED] FAILED: {failed}");
            Console.WriteLine("===========================================\n");
        }

        // Helper untuk membuat objek dummy
        private static DatabaseObject CreateObj(string id) 
            => DatabaseObject.CreateRow(id, "TestTable");

        // Helper untuk print hasil dan return status
        private static bool PrintResult(string testName, bool success, string? error = null)
        {
            if (success)
                Console.WriteLine($"[SUCCESS] {testName}\n");
            else
                Console.WriteLine($"[FAILED] {testName} - {error}\n");
            return success;
        }

        // --- TEST CASES ---

        /// <summary>
        /// Test 1: Single Transaction Commit
        /// </summary>
        private static bool Test_SingleTransaction_Commit()
        {
            string name = "Test 1: Single Transaction Commit";
            try
            {
                var manager = new TimestampOrderingManager();
                var obj1 = CreateObj("1");

                int tx1 = manager.BeginTransaction();
                
                // Read and Write operations
                manager.ValidateObject(Action.CreateReadAction(obj1, tx1));
                manager.ValidateObject(Action.CreateWriteAction(obj1, tx1));

                // Commit
                bool committed = manager.CommitTransaction(tx1);

                if (!committed) throw new Exception("Transaksi tunggal gagal commit.");
                
                var status = manager.GetTransactionStatus(tx1);
                if (status != TransactionStatus.Committed && status != TransactionStatus.Terminated) 
                    throw new Exception($"Status transaksi salah. Expected: Committed/Terminated, Actual: {status}");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 2: Successful READ operation (TS(T) >= WTS(X))
        /// </summary>
        private static bool Test_ReadOperation_Success()
        {
            string name = "Test 2: READ operation success";
            try
            {
                var manager = new TimestampOrderingManager();
                var obj = CreateObj("1");

                int txn = manager.BeginTransaction();
                var response = manager.ValidateObject(Action.CreateReadAction(obj, txn));

                if (!response.Allowed)
                    throw new Exception($"READ was denied: {response.Reason}");

                if (response.Status != Response.ResponseStatus.Granted)
                    throw new Exception($"Expected status Granted, got {response.Status}");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 3: READ abort when TS(T) < WTS(X) (reading obsolete data)
        /// </summary>
        private static bool Test_ReadOperation_Abort_ObsoleteRead()
        {
            string name = "Test 3: READ abort (obsolete read)";
            try
            {
                var manager = new TimestampOrderingManager();
                var obj = CreateObj("1");

                // T1 starts (TS=1)
                int txn1 = manager.BeginTransaction();
                // T2 starts (TS=2)
                int txn2 = manager.BeginTransaction();

                // T2 writes first (WTS(X) = 2)
                var writeResponse = manager.ValidateObject(Action.CreateWriteAction(obj, txn2));
                if (!writeResponse.Allowed)
                    throw new Exception("T2 WRITE should be allowed");

                // T1 tries to read (TS(T1) = 1 < WTS(X) = 2) - should abort
                var readResponse = manager.ValidateObject(Action.CreateReadAction(obj, txn1));

                if (readResponse.Allowed)
                    throw new Exception("READ should be denied (obsolete read)");

                if (readResponse.Status != Response.ResponseStatus.Denied)
                    throw new Exception($"Expected status Denied, got {readResponse.Status}");

                // Verify T1 is aborted
                if (manager.IsTransactionActive(txn1))
                    throw new Exception("T1 should be aborted");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 4: WRITE abort when TS(T) < RTS(X) (too late to write)
        /// </summary>
        private static bool Test_WriteOperation_Abort_TooLate()
        {
            string name = "Test 4: WRITE abort (too late to write)";
            try
            {
                var manager = new TimestampOrderingManager();
                var obj = CreateObj("1");

                // T1 starts (TS=1)
                int txn1 = manager.BeginTransaction();
                // T2 starts (TS=2)
                int txn2 = manager.BeginTransaction();

                // T2 reads first (RTS(X) = 2)
                var readResponse = manager.ValidateObject(Action.CreateReadAction(obj, txn2));
                if (!readResponse.Allowed)
                    throw new Exception("T2 READ should be allowed");

                // T1 tries to write (TS(T1) = 1 < RTS(X) = 2) - should abort
                var writeResponse = manager.ValidateObject(Action.CreateWriteAction(obj, txn1));

                if (writeResponse.Allowed)
                    throw new Exception("WRITE should be denied (too late to write)");

                if (writeResponse.Status != Response.ResponseStatus.Denied)
                    throw new Exception($"Expected status Denied, got {writeResponse.Status}");

                // Verify T1 is aborted
                if (manager.IsTransactionActive(txn1))
                    throw new Exception("T1 should be aborted");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 5: Thomas Write Rule - skip write when TS(T) < WTS(X)
        /// </summary>
        private static bool Test_ThomasWriteRule()
        {
            string name = "Test 5: Thomas Write Rule (skip write)";
            try
            {
                var manager = new TimestampOrderingManager();
                var obj = CreateObj("1");

                // T1 starts (TS=1)
                int txn1 = manager.BeginTransaction();
                // T2 starts (TS=2)
                int txn2 = manager.BeginTransaction();

                // T2 writes first (WTS(X) = 2)
                var writeResponse2 = manager.ValidateObject(Action.CreateWriteAction(obj, txn2));
                if (!writeResponse2.Allowed)
                    throw new Exception("T2 WRITE should be allowed");

                // T1 tries to write (TS(T1) = 1 < WTS(X) = 2)
                // Thomas Write Rule: skip write but allow transaction to continue
                var writeResponse1 = manager.ValidateObject(Action.CreateWriteAction(obj, txn1));

                // Write should be "allowed" but skipped (implementation detail)
                if (!writeResponse1.Allowed)
                    throw new Exception("Thomas Write Rule: transaction should continue");

                // Verify T1 is still active (not aborted)
                if (!manager.IsTransactionActive(txn1))
                    throw new Exception("T1 should remain active (Thomas Write Rule)");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 6: Multiple transactions without conflicts
        /// </summary>
        private static bool Test_MultipleTransactions_NoConflict()
        {
            string name = "Test 6: Multiple transactions without conflicts";
            try
            {
                var manager = new TimestampOrderingManager();
                var objA = CreateObj("A");
                var objB = CreateObj("B");

                // Create 2 transactions
                int t1 = manager.BeginTransaction(); // TS=1
                int t2 = manager.BeginTransaction(); // TS=2

                // T1: READ(A), WRITE(B)
                var r1 = manager.ValidateObject(Action.CreateReadAction(objA, t1));
                if (!r1.Allowed) throw new Exception("T1 READ(A) failed");

                var w1 = manager.ValidateObject(Action.CreateWriteAction(objB, t1));
                if (!w1.Allowed) throw new Exception("T1 WRITE(B) failed");

                // T2: READ(B), WRITE(A) - should succeed as operations are in timestamp order
                var r2 = manager.ValidateObject(Action.CreateReadAction(objB, t2));
                if (!r2.Allowed) throw new Exception("T2 READ(B) failed");

                var w2 = manager.ValidateObject(Action.CreateWriteAction(objA, t2));
                if (!w2.Allowed) throw new Exception("T2 WRITE(A) failed");

                // Both should commit successfully
                if (!manager.CommitTransaction(t1)) 
                    throw new Exception("T1 commit failed");
                if (!manager.CommitTransaction(t2)) 
                    throw new Exception("T2 commit failed");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }
    }
}
