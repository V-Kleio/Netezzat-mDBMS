namespace mDBMS.FailureRecovery
{
    /// <summary>
    /// Simple test runner for FRM
    /// Usage: cd src/mDBMS.FailureRecovery && dotnet run
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting FRM Mock Tests...\n");

            FRM_MockTest.RunAllTests();

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
