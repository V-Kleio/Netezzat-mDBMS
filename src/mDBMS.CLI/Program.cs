using System.Net;
using mDBMS.Common.Transaction;
using mDBMS.CLI;
using System.Text;

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

        if (result.Data != null && result.Data.Any())
        {
            var data = result.Data.ToList();
            var headers = data.First().Columns.Keys.ToList();
            var columnWidths = new Dictionary<string, int>();

            foreach (var header in headers)
            {
                columnWidths[header] = header.Length;
            }

            foreach (var row in data)
            {
                foreach (var header in headers)
                {
                    var value = row.Columns[header]?.ToString() ?? "NULL";
                    if (value.Length > columnWidths[header])
                    {
                        columnWidths[header] = value.Length;
                    }
                }
            }

            var headerLine = new StringBuilder("+");
            var titleLine = new StringBuilder("|");

            foreach (var header in headers)
            {
                headerLine.Append(new string('-', columnWidths[header] + 2) + "+");
                titleLine.Append($" {header.PadRight(columnWidths[header])} |");
            }

            Console.WriteLine(headerLine);
            Console.WriteLine(titleLine);
            Console.WriteLine(headerLine);

            foreach (var row in data)
            {
                var rowLine = new StringBuilder("|");
                foreach (var header in headers)
                {
                    var value = row.Columns[header]?.ToString() ?? "NULL";
                    rowLine.Append($" {value.PadRight(columnWidths[header])} |");
                }
                Console.WriteLine(rowLine);
            }
            
            Console.WriteLine(headerLine);
            Console.WriteLine($"{data.Count} row(s) returned.\n");
        }
    }
}
