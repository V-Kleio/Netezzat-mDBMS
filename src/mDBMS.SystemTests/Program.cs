using mDBMS.SystemTests;

namespace mDBMS.SystemTests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════════╗");
            Console.WriteLine("║       mDBMS SYSTEM INTEGRATION TESTS                  ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════════╝\n");

            int passed = 0;
            int failed = 0;

            // Test 1: Query Processor + Storage Manager Integration
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("TEST 1: Query Processor & Storage Manager Integration");
            Console.WriteLine(new string('=', 70));
            if (SystemTest1_QueryProcessorStorage.Run())
                passed++;
            else
                failed++;

            // Test 2: Failure Recovery Manager + Buffer Pool
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("TEST 2: Failure Recovery Manager & Buffer Pool");
            Console.WriteLine(new string('=', 70));
            if (SystemTest2_RecoveryBuffer.Run())
                passed++;
            else
                failed++;

            // Test 3: Query Optimizer + Parser
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("TEST 3: Query Optimizer & SQL Parser");
            Console.WriteLine(new string('=', 70));
            if (SystemTest3_OptimizerParser.Run())
                passed++;
            else
                failed++;

            // Test 4: CCM Timestamp Ordering + Query Operations
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("TEST 4: CCM (Timestamp Ordering) + Query Operations");
            Console.WriteLine(new string('=', 70));
            if (SystemTest4_CCMTimestamp.Run())
                passed++;
            else
                failed++;

            // Test 5: CCM Two-Phase Locking + Transaction Logging
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("TEST 5: CCM (Two-Phase Locking) + Transaction Logging");
            Console.WriteLine(new string('=', 70));
            if (SystemTest5_CCMLogging.Run())
                passed++;
            else
                failed++;

            // Summary
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine($"SYSTEM TEST RESULTS");
            Console.WriteLine(new string('=', 70));
            Console.WriteLine($"Total Tests: {passed + failed}");
            Console.WriteLine($"[SUCCESS] Passed: {passed}");
            Console.WriteLine($"[FAILED] Failed: {failed}");
            Console.WriteLine($"Success Rate: {(passed * 100.0 / (passed + failed)):F1}%");
            Console.WriteLine(new string('=', 70));

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
