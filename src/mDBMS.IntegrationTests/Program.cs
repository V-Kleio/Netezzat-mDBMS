using System;
using mDBMS.FailureRecovery.Tests;

namespace mDBMS.IntegrationTests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("===========================================");
            Console.WriteLine("  SM <-> FRM Integration Test Suite");
            Console.WriteLine("  Testing: Buffer, Disk I/O, and Logging");
            Console.WriteLine("===========================================\n");

            try
            {
                Integration_SM_FRM_Test.RunAllTests();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[FAILED] Fatal error: {ex.Message}");
                Console.WriteLine($"Stack Trace:\n{ex.StackTrace}");
                Environment.Exit(1);
            }
        }
    }
}
