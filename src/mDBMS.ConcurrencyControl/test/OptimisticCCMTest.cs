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

        private static void PrintResult(string testName, bool success, string? error = null)
        {
            if (success)
                Console.WriteLine($"[SUCCESS] {testName}\n");
            else
                Console.WriteLine($"[FAILED] {testName} - {error}\n");
        }

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

                // Fase Read: Selalu Allowed di OCC
                occ.ValidateObject(Action.CreateReadAction(obj1, tx1));
                occ.ValidateObject(Action.CreateWriteAction(obj1, tx1));

                // Fase Validate & Write
                bool committed = occ.CommitTransaction(tx1);

                if (!committed) throw new Exception("Transaksi tunggal gagal commit.");
                if (occ.GetTransactionStatus(tx1) != TransactionStatus.Committed) 
                    throw new Exception("Status transaksi bukan Committed.");

                PrintResult(name, true);
                return true;
            }
            catch (Exception ex)
            {
                PrintResult(name, false, ex.Message);
                return false;
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

                // T1 akses A, T2 akses B
                occ.ValidateObject(Action.CreateWriteAction(objA, t1));
                occ.ValidateObject(Action.CreateWriteAction(objB, t2));

                // Commit T1
                if (!occ.CommitTransaction(t1)) 
                    throw new Exception("T1 gagal commit.");

                // Commit T2 (validasi terhadap T1, tapi set objek beda)
                if (!occ.CommitTransaction(t2)) 
                    throw new Exception("T2 gagal commit padahal objek berbeda.");

                PrintResult(name, true);
                return true;
            }
            catch (Exception ex)
            {
                PrintResult(name, false, ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Test 3: Read-Write Conflict
        /// </summary>
        private static bool Test_ReadWrite_Conflict()
        {
            string name = "Test 3: Read-Write Conflict (Validation Fail)";
            try
            {
                var occ = new OptimisticConcurrencyManager();
                var objA = CreateObj("A");

                // 1. T1 Mulai (Timestamp awal kecil)
                int t1 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateReadAction(objA, t1)); // T1 baca A

                // 2. T2 Mulai
                int t2 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateWriteAction(objA, t2)); // T2 tulis A

                // 3. T2 Commit duluan (Sukses)
                if (!occ.CommitTransaction(t2)) 
                    throw new Exception("T2 seharusnya sukses commit.");

                // 4. T1 Coba Commit
                // Saat validasi, OCC melihat T2 sudah commit dan FinishTS(T2) > StartTS(T1).
                // Cek konflik: WriteSet(T2) intersect ReadSet(T1) pada "A".
                bool t1Result = occ.CommitTransaction(t1);

                if (t1Result) 
                {
                    PrintResult(name, false, "T1 seharusnya gagal (Abort) karena konflik R-W.");
                    return false;
                }

                if (occ.GetTransactionStatus(t1) != TransactionStatus.Aborted && 
                    occ.GetTransactionStatus(t1) != TransactionStatus.Terminated)
                {
                     throw new Exception($"Status T1 harusnya Aborted/Terminated, tapi {occ.GetTransactionStatus(t1)}");
                }

                PrintResult(name, true);
                return true;
            }
            catch (Exception ex)
            {
                PrintResult(name, false, ex.Message);
                return false;
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
                occ.ValidateObject(Action.CreateWriteAction(objA, t1)); // T1 Write

                int t2 = occ.BeginTransaction();
                occ.ValidateObject(Action.CreateWriteAction(objA, t2)); // T2 Write

                // T2 Commit
                occ.CommitTransaction(t2);

                // T1 Commit -> Harus gagal karena WriteSet(T2) intersect WriteSet(T1)
                bool t1Success = occ.CommitTransaction(t1);

                if (t1Success)
                {
                    PrintResult(name, false, "T1 seharusnya gagal karena konflik W-W.");
                    return false;
                }

                PrintResult(name, true);
                return true;
            }
            catch (Exception ex)
            {
                PrintResult(name, false, ex.Message);
                return false;
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

                if (!result) throw new Exception("Abort mengembalikan false.");
                
                var status = occ.GetTransactionStatus(t1);
                
                if (status != TransactionStatus.Aborted && status != TransactionStatus.Terminated)
                    throw new Exception($"Status salah: {status}");

                PrintResult(name, true);
                return true;
            }
            catch (Exception ex)
            {
                PrintResult(name, false, ex.Message);
                return false;
            }
        }
    }
}