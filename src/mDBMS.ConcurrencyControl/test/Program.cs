namespace mDBMS.ConcurrencyControl.Tests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting Concurrency Control Tests...\n");

            // Jalankan Test Suite
            OptimisticCCMTest.RunAllTests();

            Console.WriteLine("\nPress any key to exit...");
            // Console.ReadKey(); // Opsional, uncomment jika ingin window tetap terbuka
        }
    }
}