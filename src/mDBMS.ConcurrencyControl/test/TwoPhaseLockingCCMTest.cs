using mDBMS.Common.Transaction;
using mDBMS.ConcurrencyControl;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.ConcurrencyControl.Tests
{
    // Unit test buat testing TwoPhaseLockingManager
    public static class TwoPhaseLockingCCMTest
    {
        public static void RunAllTests()
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  TWO-PHASE LOCKING (2PL) TESTS");
            Console.WriteLine("===========================================\n");

            int passed = 0;
            int failed = 0;

            // Jalanin semua test cases
            if (Test_SingleTransaction_Commit()) passed++; else failed++;
            if (Test_SharedLock_Compatible()) passed++; else failed++;
            if (Test_ExclusiveLock_Conflict()) passed++; else failed++;
            if (Test_SharedExclusive_Conflict()) passed++; else failed++;
            if (Test_LockUpgrade_Success()) passed++; else failed++;
            if (Test_LockUpgrade_Conflict()) passed++; else failed++;
            if (Test_Deadlock_Detection()) passed++; else failed++;
            if (Test_IsTransactionActive()) passed++; else failed++;
            if (Test_Abort_Transaction()) passed++; else failed++;

            Console.WriteLine("===========================================");
            Console.WriteLine($"TOTAL: {passed + failed} tests");
            Console.WriteLine($"[SUCCESS] PASSED: {passed}");
            Console.WriteLine($"[FAILED] FAILED: {failed}");
            Console.WriteLine("===========================================\n");
        }

        // Helper buat bikin dummy database object
        private static DatabaseObject CreateObj(string id) 
            => DatabaseObject.CreateRow(id, "TestTable");

        // Helper buat print hasil test
        private static bool PrintResult(string testName, bool success, string? error = null)
        {
            if (success)
                Console.WriteLine($"[SUCCESS] {testName}\n");
            else
                Console.WriteLine($"[FAILED] {testName} - {error}\n");
            return success;
        }

        /// <summary>
        /// Test 1: Transaksi tunggal yang commit dengan sukses
        /// Cek: acquire S-lock, upgrade ke X-lock, lalu commit berhasil
        /// </summary>
        private static bool Test_SingleTransaction_Commit()
        {
            string name = "Test 1: Single Transaction Commit";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var obj1 = CreateObj("1");

                int tx1 = tpl.BeginTransaction();

                var readResp = tpl.ValidateObject(Action.CreateReadAction(obj1, tx1));
                if (!readResp.Allowed) throw new Exception("Read lock gagal.");

                var writeResp = tpl.ValidateObject(Action.CreateWriteAction(obj1, tx1));
                if (!writeResp.Allowed) throw new Exception("Write lock gagal.");

                bool committed = tpl.CommitTransaction(tx1);
                if (!committed) throw new Exception("Commit gagal.");

                var status = tpl.GetTransactionStatus(tx1);
                if (status != TransactionStatus.Committed && status != TransactionStatus.Terminated) 
                    throw new Exception($"Status salah: {status}");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 2: Multiple S-lock bisa coexist
        /// Cek: T1 dan T2 sama-sama pegang S-lock di objek yang sama, harusnya tetep compatible
        /// </summary>
        private static bool Test_SharedLock_Compatible()
        {
            string name = "Test 2: Shared Locks Compatible";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");

                int t1 = tpl.BeginTransaction();
                int t2 = tpl.BeginTransaction();

                var r1 = tpl.ValidateObject(Action.CreateReadAction(objA, t1));
                if (!r1.Allowed) throw new Exception("T1 gagal S-lock.");

                var r2 = tpl.ValidateObject(Action.CreateReadAction(objA, t2));
                if (!r2.Allowed) throw new Exception("T2 gagal S-lock (S-S harus compatible).");

                tpl.CommitTransaction(t1);
                tpl.CommitTransaction(t2);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 3: X-lock conflict dengan X-lock lain
        /// Cek: T1 pegang X-lock terus T2 coba acquire X-lock, harus return waiting/denied
        /// </summary>
        private static bool Test_ExclusiveLock_Conflict()
        {
            string name = "Test 3: Exclusive Lock Conflicts";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");

                int t1 = tpl.BeginTransaction();
                int t2 = tpl.BeginTransaction();

                // T1 dapet X-lock
                var w1 = tpl.ValidateObject(Action.CreateWriteAction(objA, t1));
                if (!w1.Allowed) throw new Exception("T1 gagal X-lock.");

                // T2 coba ambil X-lock di objek yang sama 
                var w2 = tpl.ValidateObject(Action.CreateWriteAction(objA, t2));
                if (w2.Allowed) throw new Exception("T2 seharusnya WAITING/DENIED.");

                tpl.AbortTransaction(t1);
                tpl.AbortTransaction(t2);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 4: S-lock conflict dengan X-lock
        /// Cek: T1 pegang S-lock terus T2 coba acquire X-lock, harus waiting/denied
        /// </summary>
        private static bool Test_SharedExclusive_Conflict()
        {
            string name = "Test 4: Shared-Exclusive Conflict";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");

                int t1 = tpl.BeginTransaction();
                int t2 = tpl.BeginTransaction();

                // T1 dapet S-lock
                var r1 = tpl.ValidateObject(Action.CreateReadAction(objA, t1));
                if (!r1.Allowed) throw new Exception("T1 gagal S-lock.");

                // T2 coba ambil X-lock, harus conflict
                var w2 = tpl.ValidateObject(Action.CreateWriteAction(objA, t2));
                if (w2.Allowed) throw new Exception("T2 seharusnya WAITING/DENIED karena T1 pegang S-lock.");

                tpl.AbortTransaction(t1);
                tpl.AbortTransaction(t2);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 5: Lock upgrade dari S ke X berhasil
        /// Cek: T1 acquire S-lock terus upgrade ke X-lock harus berhasil
        /// </summary>
        private static bool Test_LockUpgrade_Success()
        {
            string name = "Test 5: Lock Upgrade Success";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");

                int t1 = tpl.BeginTransaction();

                var r1 = tpl.ValidateObject(Action.CreateReadAction(objA, t1));
                if (!r1.Allowed) throw new Exception("T1 gagal S-lock.");

                var w1 = tpl.ValidateObject(Action.CreateWriteAction(objA, t1));
                if (!w1.Allowed) throw new Exception("T1 gagal upgrade S ke X.");

                tpl.CommitTransaction(t1);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 6: Lock upgrade gagal karena ada transaksi lain
        /// Cek: T1 dan T2 pegang S-lock terus T1 coba upgrade, harus gagal karena T2 masih pegang S-lock
        /// </summary>
        private static bool Test_LockUpgrade_Conflict()
        {
            string name = "Test 6: Lock Upgrade Conflict";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");

                int t1 = tpl.BeginTransaction();
                int t2 = tpl.BeginTransaction();

                tpl.ValidateObject(Action.CreateReadAction(objA, t1));
                tpl.ValidateObject(Action.CreateReadAction(objA, t2));

                // T1 coba upgrade, harus gagal karena T2 masih hold S-lock
                var w1 = tpl.ValidateObject(Action.CreateWriteAction(objA, t1));
                if (w1.Allowed) throw new Exception("T1 seharusnya gagal upgrade karena T2 pegang S-lock.");

                tpl.AbortTransaction(t1);
                tpl.AbortTransaction(t2);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 7: Deadlock detection dengan wait-for graph
        /// Cek: T1 hold A want B, T2 hold B want A, deadlock terdeteksi dan salah satu di abort
        /// </summary>
        private static bool Test_Deadlock_Detection()
        {
            string name = "Test 7: Deadlock Detection";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                var objA = CreateObj("A");
                var objB = CreateObj("B");

                int t1 = tpl.BeginTransaction();
                int t2 = tpl.BeginTransaction();

                // T1 lock A, T2 lock B
                var t1a = tpl.ValidateObject(Action.CreateWriteAction(objA, t1));
                if (!t1a.Allowed) throw new Exception("T1 gagal lock A.");

                var t2b = tpl.ValidateObject(Action.CreateWriteAction(objB, t2));
                if (!t2b.Allowed) throw new Exception("T2 gagal lock B.");

                // T1 mau B (wait), T2 mau A (deadlock!)
                tpl.ValidateObject(Action.CreateWriteAction(objB, t1));
                tpl.ValidateObject(Action.CreateWriteAction(objA, t2));
                
                var status1 = tpl.GetTransactionStatus(t1);
                var status2 = tpl.GetTransactionStatus(t2);

                // Minimal salah satu harus di abort
                bool oneAborted = 
                    (status1 == TransactionStatus.Aborted || status1 == TransactionStatus.Failed || status1 == TransactionStatus.Terminated) ||
                    (status2 == TransactionStatus.Aborted || status2 == TransactionStatus.Failed || status2 == TransactionStatus.Terminated);

                if (!oneAborted) 
                    throw new Exception("Deadlock tidak terdeteksi, harusnya salah satu transaksi di-abort.");

                // Cleanup transaksi yang masih aktif
                if (status1 == TransactionStatus.Active || status1 == TransactionStatus.Waiting)
                    tpl.AbortTransaction(t1);
                if (status2 == TransactionStatus.Active || status2 == TransactionStatus.Waiting)
                    tpl.AbortTransaction(t2);

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 8: Method IsTransactionActive
        /// Cek: active setelah begin dan not active setelah commit
        /// </summary>
        private static bool Test_IsTransactionActive()
        {
            string name = "Test 8: IsTransactionActive";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                
                int t1 = tpl.BeginTransaction();
                
                if (!tpl.IsTransactionActive(t1))
                    throw new Exception("T1 seharusnya active setelah begin.");

                tpl.CommitTransaction(t1);

                if (tpl.IsTransactionActive(t1))
                    throw new Exception("T1 seharusnya tidak active setelah commit.");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }

        /// <summary>
        /// Test 9: Explicit abort transaksi
        /// Cek: transaksi bisa di abort manual dan status berubah jadi Aborted
        /// </summary>
        private static bool Test_Abort_Transaction()
        {
            string name = "Test 9: Explicit Abort";
            try
            {
                var tpl = new TwoPhaseLockingManager();
                int t1 = tpl.BeginTransaction();

                bool result = tpl.AbortTransaction(t1);
                if (!result) return PrintResult(name, false, "Abort mengembalikan false.");
                
                var status = tpl.GetTransactionStatus(t1);
                if (status != TransactionStatus.Aborted && status != TransactionStatus.Terminated)
                    throw new Exception($"Status salah setelah abort: {status}");

                return PrintResult(name, true);
            }
            catch (Exception ex)
            {
                return PrintResult(name, false, ex.Message);
            }
        }
    }
}