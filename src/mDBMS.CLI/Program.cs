using System.Net;
using mDBMS.Common.Transaction;
using mDBMS.CLI;

class CLI
{
    public static void Main(string[] args)
    {
        IPAddress host = IPAddress.Loopback;
        short port = 5761;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--host":
                case "-h":
                    IPAddress[] addresses = Dns.GetHostAddresses(args[++i]);
                    if (addresses.Length > 0)
                    {
                        host = addresses[0];
                    }
                    else
                    {
                        Console.Error.Write("User assigned host address is not valid!");
                        Environment.ExitCode = -1;
                        return;
                    }
                break;

                case "--port":
                case "-p":
                    if (short.TryParse(args[++i], out short assignedPort))
                    {
                        port = assignedPort;
                    }
                    else
                    {
                        Console.Error.Write("User assigned port number is not valid!");
                        Environment.ExitCode = -1;
                        return;
                    }
                break;
            }
        }

        ProcessorProxy proxy = new(new(host, port));

        Console.WriteLine("mDBMS CLI siap digunakan. Ketik EXIT untuk keluar.");

        while (true)
        {
            Console.Write("mDBMS > ");
            var input = Console.ReadLine();

            if (input is null)
            {
                break;
            }

            if (string.Equals(input.Trim(), "EXIT", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Sampai jumpa!");
                break;
            }

            var result = proxy.ExecuteQuery(input, -1);
            PrintResult(result);
        }
    }

    static void PrintResult(ExecutionResult result)
    {
        var status = result.Success ? "SUCCESS" : "ERROR";
        Console.WriteLine($"[{status}] {result.Message}");

        if (result.Data != null)
        {
            Console.WriteLine("\nHasil:");
            foreach (var row in result.Data)
            {
                Console.WriteLine(string.Join(" | ", row.Columns.Select(kv => $"{kv.Key}: {kv.Value}")));
            }
            Console.WriteLine();
        }
    }
}
