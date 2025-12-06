using mDBMS.Common.Interfaces;
using mDBMS.Common.Net;
using mDBMS.Common.Transaction;
using mDBMS.ConcurrencyControl;
using mDBMS.FailureRecovery;
using mDBMS.QueryOptimizer;
using mDBMS.QueryProcessor;
using mDBMS.StorageManager;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

class Server
{
    public static void Main(string[] args)
    {
        short port = 5761;
        ConcurrencyProtocol ccmProtocol = ConcurrencyProtocol.TwoPhaseLocking;


        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" || args[i] == "-p")
            {
                if (i + 1 < args.Length && short.TryParse(args[i + 1], out short assignedPort))
                {
                    port = assignedPort;
                    i++;
                }
                else
                {
                    Console.Error.WriteLine("Invalid port number!");
                    Environment.ExitCode = -1;
                    return;
                }
            }
            else if (args[i] == "--ccm-strategy" || args[i] == "-s")
            {
                if (i + 1 < args.Length)
                {
                    switch (args[i + 1].ToUpper())
                    {
                        case "2PL":
                        case "TWOPHASELOCKING":
                            ccmProtocol = ConcurrencyProtocol.TwoPhaseLocking;
                            break;
                        case "TO":
                        case "TIMESTAMPORDERING":
                            ccmProtocol = ConcurrencyProtocol.TimestampOrdering;
                            break;
                        case "OCC":
                        case "OPTIMISTIC":
                        case "OPTIMISTICVALIDATION":
                            ccmProtocol = ConcurrencyProtocol.OptimisticValidation;
                            break;
                        default:
                            Console.Error.WriteLine($"Unknown CCM strategy: {args[i + 1]}");
                            Console.Error.WriteLine("Valid options: 2PL, TO, OCC");
                            Environment.ExitCode = -1;
                            return;
                    }
                    i++;
                }
                else
                {
                    Console.Error.WriteLine("CCM strategy not specified!");
                    Environment.ExitCode = -1;
                    return;
                }
            }
            else if (short.TryParse(args[i], out short legacyPort))
            {
                port = legacyPort;
            }
        }


        int connectionTimeout = 1000;
        int initialBufferSize = 4096;

        var qpProxy = LateProxy<IQueryProcessor>.Create();
        var bmProxy = LateProxy<IBufferManager>.Create();

        var sm = new StorageEngine(bmProxy);
        var qo = new QueryOptimizerEngine(sm);
        var ccm = new ConcurrencyControlManager(ccmProtocol);
        var frm = new FailureRecoveryManager(qpProxy, sm);
        var qp = new QueryProcessor(sm, qo, ccm, frm);

        ((LateProxy<IQueryProcessor>)qpProxy).SetTarget(qp);
        ((LateProxy<IBufferManager>)bmProxy).SetTarget(frm);

        IPEndPoint endpoint = new(IPAddress.Loopback, port);

        using TcpListener listener = new(endpoint);
        listener.Start();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\nShutting down server...");
            listener.Stop();
            Environment.Exit(0);
        };
        Console.WriteLine($"Server listening on {endpoint}. Press Ctrl+C to stop.");

        while (true)
        {
            TcpClient? handler = null;
            NetworkStream? stream = null;

            try
            {
                handler = listener.AcceptTcpClient();
                handler.ReceiveTimeout = connectionTimeout;
                stream = handler.GetStream();

                byte[] buffer = new byte[initialBufferSize];
                int length = 0;

                Stopwatch stopwatch = new();
                stopwatch.Start();

                length += stream.Read(buffer, 0, buffer.Length);
                while (length == buffer.Length)
                {
                    stopwatch.Stop();

                    Array.Resize(ref buffer, buffer.Length * 2);
                    length += stream.Read(buffer, length, buffer.Length - length);

                    if (stopwatch.ElapsedMilliseconds >= connectionTimeout)
                    {
                        break;
                    }

                    stopwatch.Start();
                }

                if (length == 0)
                {
                    Console.WriteLine("Client disconnected without sending data");
                    continue;
                }

                var (query, transactionId) = QueryDecoder.Decode(buffer, 0, length);

                if (string.IsNullOrWhiteSpace(query))
                {
                    ExecutionResult emptyResult = new()
                    {
                        Query = query,
                        Success = false,
                        Message = "Empty query"
                    };
                    byte[] emptyResponse = ExecutionResultEncoder.Encode(emptyResult);
                    stream.Write(emptyResponse, 0, emptyResponse.Length);
                    continue;
                }

                Console.WriteLine($"Received query: {query} with transaction ID: {transactionId}");

                ExecutionResult result = qp.ExecuteQuery(query, transactionId);
                byte[] response = ExecutionResultEncoder.Encode(result);

                stream.Write(response, 0, response.Length);
            }
            catch (SocketException e)
            {
                Console.WriteLine($"Socket error: {e.Message}");
            }
            catch (IOException e)
            {
                Console.WriteLine($"IO error: {e.Message}");
            }
            catch (ArgumentException e)
            {
                Console.WriteLine($"Argument error: {e.Message}");

                if (stream != null)
                {
                    try
                    {
                        ExecutionResult errorResult = new()
                        {
                            Query = "",
                            Success = false,
                            Message = $"Server error: {e.Message}"
                        };
                        byte[] errorResponse = ExecutionResultEncoder.Encode(errorResult);
                        stream.Write(errorResponse, 0, errorResponse.Length);
                    }
                    catch
                    {
                        // Stream might be broken, ignore
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e.Message}");
            }
            finally
            {
                stream?.Dispose();
                handler?.Dispose();
            }
        }
    }
}
