namespace mDBMS.ConcurrencyControl.Tests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting Concurrency Control Tests...\n");

            // Test Optimistic
            OptimisticCCMTest.RunAllTests();

            Console.WriteLine("\n");

            // Test Two-Phase Locking
            TwoPhaseLockingCCMTest.RunAllTests();

            Console.WriteLine("\nAll tests completed!");
        }
    }
}