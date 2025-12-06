namespace mDBMS.ConcurrencyControl.Tests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting Concurrency Control Tests...\n");

            // Optimistic Concurrency Control
            OptimisticCCMTest.RunAllTests();

            // Timestamp Ordering Protocol
            TimestampCCMTest.RunAllTests();

            Console.WriteLine("\nPress any key to exit...");
            // Console.ReadKey();
        }
    }
}