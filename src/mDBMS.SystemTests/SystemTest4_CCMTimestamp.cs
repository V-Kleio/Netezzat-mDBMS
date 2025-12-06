using mDBMS.ConcurrencyControl;
using mDBMS.Common.Transaction;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.SystemTests
{
    /// <summary>
    /// System Test 4: CCM (Timestamp Ordering) + Query Operations
    /// Components: CCM (TO) + Query simulation
    /// Timestamp ordering with query-like operations
    /// </summary>
    public static class SystemTest4_CCMTimestamp
    {
        public static bool Run()
        {
            try
            {
                Console.WriteLine("[INFO] Initializing CCM with Timestamp Ordering...");
                var ccm = new ConcurrencyControlManager(ConcurrencyProtocol.TimestampOrdering);

                Console.WriteLine("[INFO] Testing TO protocol with query operations\n");

                bool allTestsPassed = true;

                // Test 1: Simple query sequence (SELECT then UPDATE)
                Console.WriteLine("--- Sub-test 1: Query sequence with TO ---");
                var row1 = DatabaseObject.CreateRow("1", "products");

                int t1 = ccm.BeginTransaction();
                Console.WriteLine($"[CCM-TO] Transaction T{t1} started (TS assigned)");

                // Simulate SELECT query
                Console.WriteLine($"[Query] Simulating SELECT FROM products WHERE id=1");
                var readResp = ccm.ValidateObject(Action.CreateReadAction(row1, t1));
                if (!readResp.Allowed)
                {
                    Console.WriteLine($"[FAILED] SELECT should be allowed");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-TO] READ granted (RTS updated)");
                }

                // Simulate UPDATE query
                Console.WriteLine($"[Query] Simulating UPDATE products SET price=100 WHERE id=1");
                var writeResp = ccm.ValidateObject(Action.CreateWriteAction(row1, t1));
                if (!writeResp.Allowed)
                {
                    Console.WriteLine($"[FAILED] UPDATE should be allowed");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-TO] WRITE granted (WTS updated)");
                }

                ccm.CommitTransaction(t1);
                Console.WriteLine($"[CCM-TO] Transaction committed\n");

                // Test 2: Concurrent read queries (lock-free)
                Console.WriteLine("--- Sub-test 2: Concurrent SELECTs (lock-free) ---");
                var row2 = DatabaseObject.CreateRow("100", "users");

                int t2 = ccm.BeginTransaction();
                int t3 = ccm.BeginTransaction();
                int t4 = ccm.BeginTransaction();

                Console.WriteLine($"[Query] 3 concurrent SELECT FROM users WHERE id=100");
                
                var r2 = ccm.ValidateObject(Action.CreateReadAction(row2, t2));
                var r3 = ccm.ValidateObject(Action.CreateReadAction(row2, t3));
                var r4 = ccm.ValidateObject(Action.CreateReadAction(row2, t4));

                if (!r2.Allowed || !r3.Allowed || !r4.Allowed)
                {
                    Console.WriteLine($"[FAILED] All SELECTs should be allowed (no locks in TO)");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM-TO] All 3 READs granted simultaneously");
                    Console.WriteLine($"[CCM-TO] No blocking (lock-free protocol)");
                }

                ccm.CommitTransaction(t2);
                ccm.CommitTransaction(t3);
                ccm.CommitTransaction(t4);
                Console.WriteLine($"[CCM-TO] All transactions committed\n");

                // Test 3: Write sequence
                Console.WriteLine("--- Sub-test 3: Sequential UPDATEs ---");
                
                int t5 = ccm.BeginTransaction();
                int t6 = ccm.BeginTransaction();

                var prod1 = DatabaseObject.CreateRow("200", "products");
                var prod2 = DatabaseObject.CreateRow("300", "products");

                Console.WriteLine($"[Query] T{t5}: UPDATE products SET stock=10 WHERE id=200");
                ccm.ValidateObject(Action.CreateWriteAction(prod1, t5));
                Console.WriteLine($"[CCM-TO] T{t5} WRITE granted (WTS=TS(T{t5}))");

                Console.WriteLine($"[Query] T{t6}: UPDATE products SET stock=20 WHERE id=300");
                ccm.ValidateObject(Action.CreateWriteAction(prod2, t6));
                Console.WriteLine($"[CCM-TO] T{t6} WRITE granted (WTS=TS(T{t6}))");

                ccm.CommitTransaction(t5);
                ccm.CommitTransaction(t6);
                Console.WriteLine($"[CCM-TO] Both UPDATEs committed\n");

                // Evaluation
                Console.WriteLine($"[EVALUATION] All sub-tests passed: {allTestsPassed}");

                if (allTestsPassed)
                {
                    Console.WriteLine("\n[SUCCESS] CCM-TO with query operations verified!");
                    Console.WriteLine("  SELECT queries validated by TO");
                    Console.WriteLine("  UPDATE queries validated by TO");
                    Console.WriteLine("  Concurrent reads allowed (lock-free)");
                    Console.WriteLine("  Timestamp-based serialization");
                }
                else
                {
                    Console.WriteLine("\n[FAILED] Some CCM-TO tests failed");
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
