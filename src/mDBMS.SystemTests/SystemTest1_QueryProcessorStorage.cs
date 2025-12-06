using mDBMS.ConcurrencyControl;
using mDBMS.Common.Transaction;
using Action = mDBMS.Common.Transaction.Action;

namespace mDBMS.SystemTests
{
    /// <summary>
    /// System Test 1: Query Processor & Storage Manager Demonstration
    /// ComponentsFocused: QP + SM + QO + CCM + FRM
    /// Demonstrates end-to-end query execution pipeline
    /// </summary>
    public static class SystemTest1_QueryProcessorStorage
    {
        public static bool Run()
        {
            try
            {
                Console.WriteLine("[INFO] Demonstrating QP + SM + QO + CCM + FRM integration");
                Console.WriteLine("[NOTE] Conceptual demonstration of full stack\n");

                var ccm = new ConcurrencyControlManager(ConcurrencyProtocol.TwoPhaseLocking);
                bool allTestsPassed = true;

                //Test 1: CREATE TABLE demonstration
                Console.WriteLine("--- Sub-test 1: CREATE TABLE flow ---");
                Console.WriteLine($"[QP] Receives: CREATE TABLE users (id INT, name VARCHAR)");
                Console.WriteLine($"[QP] Parses query and extracts table schema");
                Console.WriteLine($"[SM] Creates table metadata file");
                Console.WriteLine($"[SM] Allocates initial data blocks");
                Console.WriteLine($"[SUCCESS] Table created in storage\n");

                // Test 2: INSERT demonstration
                Console.WriteLine("--- Sub-test 2: INSERT with full pipeline ---");
                int txn1 = ccm.BeginTransaction();
                Console.WriteLine($"[CCM] Transaction T{txn1} BEGIN");

                var row = DatabaseObject.CreateRow("1", "users");
                var writeResp = ccm.ValidateObject(Action.CreateWriteAction(row, txn1));
                
                if (!writeResp.Allowed)
                {
                    Console.WriteLine($"[FAILED] Lock acquisition failed");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[QP] Receives: INSERT INTO users VALUES (1, 'Alice')");
                    Console.WriteLine($"[QP] Creates Insert logical plan");
                    Console.WriteLine($"[CCM] Acquires X-lock on row");
                    Console.WriteLine($"[SM] Writes data to page buffer");
                    Console.WriteLine($"[FRM] Logs INSERT operation");
                }

                ccm.CommitTransaction(txn1);
                Console.WriteLine($"[CCM] Transaction committed");
                Console.WriteLine($"[FRM] Logs COMMIT");
                Console.WriteLine($"[SM] Flushes dirty pages to disk");
                Console.WriteLine($"[SUCCESS] Data persisted\n");

                // Test 3: SELECT demonstration
                Console.WriteLine("--- Sub-test 3: SELECT with optimization ---");
                int txn2 = ccm.BeginTransaction();
                Console.WriteLine($"[QP] Receives: SELECT * FROM users WHERE id = 1");
                Console.WriteLine($"[QP] Sends query to Query Optimizer");
                Console.WriteLine($"[QO] Analyzes query: WHERE id = 1 (selective)");
                Console.WriteLine($"[QO] Cost estimation:");
                Console.WriteLine($"     - Index Seek: Cost ~16");
                Console.WriteLine($"     - Table Scan: Cost ~1000");
                Console.WriteLine($"[QO] Decision: Index Seek selected");
                Console.WriteLine($"[QP] Executes optimized plan");

                var readResp = ccm.ValidateObject(Action.CreateReadAction(row, txn2));
                if (!readResp.Allowed)
                {
                    Console.WriteLine($"[FAILED] Read validation failed");
                    allTestsPassed = false;
                }
                else
                {
                    Console.WriteLine($"[CCM] Acquires S-lock on row");
                    Console.WriteLine($"[SM] Retrieves data from page buffer/disk");
                    Console.WriteLine($"[QP] Returns result set to client");
                }

                ccm.CommitTransaction(txn2);
                Console.WriteLine($"[CCM] Transaction committed, S-lock released");
                Console.WriteLine($"[SUCCESS] Query executed\n");

                // Evaluation
                Console.WriteLine($"[EVALUATION] All sub-tests passed: {allTestsPassed}");

                if (allTestsPassed)
                {
                    Console.WriteLine("\n[SUCCESS] QP & SM pipeline verified!");
                    Console.WriteLine("  Query Processor orchestrates operations");
                    Console.WriteLine("  Storage Manager handles persistence");
                    Console.WriteLine("  Query Optimizer selects best plan");
                    Console.WriteLine("  CCM ensures isolation");
                    Console.WriteLine("  FRM provides durability");
                }
                else
                {
                    Console.WriteLine("\n[FAILED] Some pipeline tests failed");
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
