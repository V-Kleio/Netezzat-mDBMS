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
            Console.WriteLine("Starting FRM Tests...\n");

            // Run mock tests (data operations)
            FRM_MockTest.RunAllTests();

            Console.WriteLine("\n");

            // Run transaction control tests (BEGIN/COMMIT/ABORT)
            mDBMS.FailureRecovery.Tests.FRM_TransactionControl_Test.RunAllTests();

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
