using mDBMS.Common.Transaction;
using mDBMS.ConcurrencyControl;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.ConcurrencyControl.Tests
{
    public static class OptimisticCCMTest
    {
        public static void RunAllTests()
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  OPTIMISTIC CONCURRENCY CONTROL TESTS");
            Console.WriteLine("===========================================\n");

            int passed = 0;
            int failed = 0;

            if (Test_SingleTransaction_Commit()) passed++; else failed++;
            if (Test_NonConflicting_Transactions()) passed++; else failed++;
            if (Test_ReadWrite_Conflict()) passed++; else failed++;
            if (Test_WriteWrite_Conflict()) passed++; else failed++;
            if (Test_Abort_Transaction()) passed++; else failed++;

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
                var occ = new OptimisticConcurrencyManager();
                var obj1 = CreateObj("1");

                int tx1 = occ.BeginTransaction();

                // Fase Read
                occ.ValidateObject(Action.CreateReadAction(obj1, tx1));
                occ.ValidateObject(Action.CreateWriteAction(obj1, tx1));

                // Fase Validate & Write
                bool committed = occ.CommitTransaction(tx1);

                if (!committed) throw new Exception("Transaksi tunggal gagal commit.");
                
                // VALIDASI STATUS: OCC mengubah status Committed -> Terminated dengan cepat.
                // Jadi kita terima keduanya sebagai sukses.
                var status = occ.GetTransactionStatus(tx1);
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
        /// Test 2: Non-Conflicting Transactions
        /// </summary>
        private static bool Test_NonConflicting_Transactions()
        {
            string name = "Test 2: Non-Conflicting Transactions";
            try
            {
                var occ = new OptimisticConcurrencyManager();
                var objA = CreateObj("A");
                var objB = CreateObj("B");

                int t1 = occ.BeginTransaction();
                int t2 = occ.BeginTransaction();

                // T1 akses A, T2 akses B (Beda objek, aman)
                occ.ValidateObject(Action.CreateWriteAction(objA, t1));
                occ.ValidateObject(Action.CreateWriteAction(objB, t2));

                if (!occ.CommitTransaction(t1)) throw new Exception("T1 gagal.");
                if (!occ.CommitTransaction(t2)) throw new Exception("T2 gagal.");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 3: Read-Write Conflict
        /// </summary>
        private static bool Test_ReadWrite_Conflict()
        {
            string name = "Test 3: Read-Write Conflict";
            try
            {
                var occ = new OptimisticConcurrencyManager();
                var objA = CreateObj("A");

                int t1 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateReadAction(objA, t1)); // T1 baca A

                int t2 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateWriteAction(objA, t2)); // T2 tulis A

                // T2 selesai duluan
                if (!occ.CommitTransaction(t2)) throw new Exception("T2 gagal commit.");

                // T1 harus gagal karena data A yang dibaca sudah usang (ditulis T2)
                bool t1Result = occ.CommitTransaction(t1);

                if (t1Result) return PrintResult(name, false, "T1 seharusnya gagal (Abort) karena konflik R-W.");
                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 4: Write-Write Conflict
        /// </summary>
        private static bool Test_WriteWrite_Conflict()
        {
            string name = "Test 4: Write-Write Conflict";
            try
            {
                var occ = new OptimisticConcurrencyManager();
                var objA = CreateObj("A");

                int t1 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateWriteAction(objA, t1));

                int t2 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateWriteAction(objA, t2));

                // T2 commit duluan
                occ.CommitTransaction(t2);

                // T1 harus gagal karena konflik penulisan pada A
                bool t1Success = occ.CommitTransaction(t1);

                if (t1Success) return PrintResult(name, false, "T1 seharusnya gagal karena konflik W-W.");
                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 5: Explicit Abort
        /// </summary>
        private static bool Test_Abort_Transaction()
        {
            string name = "Test 5: Explicit Abort";
            try
            {
                var occ = new OptimisticConcurrencyManager();
                int t1 = occ.BeginTransaction();

                bool result = occ.AbortTransaction(t1);

                if (!result) return PrintResult(name, false, "Abort mengembalikan false.");
                
                var status = occ.GetTransactionStatus(t1);
                if (status != TransactionStatus.Aborted && status != TransactionStatus.Terminated)
                    throw new Exception($"Status salah: {status}");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }
    }
}