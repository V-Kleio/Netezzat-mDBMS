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

class Server
{
    public static void Main(string[] args)
    {
        short port = 5761;

        if (args.Length > 0)
        {
            if (short.TryParse(args[0], out short assignedPort))
            {
                port = assignedPort;
            }
            else
            {
                Console.Error.Write("User assigned port number is not valid!");
                Environment.ExitCode = -1;
                return;
            }
        }

        int connectionTimeout = 1000;
        int initialBufferSize = 4096;

        var qpProxy = LateProxy<IQueryProcessor>.Create();

        var sm = new StorageEngine();
        var qo = new QueryOptimizerEngine(sm);
        var ccm = new ConcurrencyControlManager();
        var frm = new FailureRecoveryManager(qpProxy, sm);
        var qp = new QueryProcessor(sm, qo, ccm, frm);

        var queryDecoder = new QueryDecoder();
        var resultEncoder = new ExecutionResultEncoder();

        ((LateProxy<IQueryProcessor>) qpProxy).SetTarget(qp);

        IPEndPoint endpoint = new(IPAddress.Loopback, port);

        using (TcpListener listener = new(endpoint))
        {
            listener.Start();
            Console.WriteLine($"Server listening on {endpoint}");

            while (true)
            {
                try
                {
                    using (TcpClient handler = listener.AcceptTcpClient())
                    {
                        handler.ReceiveTimeout = connectionTimeout;
                        using (NetworkStream stream = handler.GetStream())
                        {
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
        
                            var (query, transactionId) = queryDecoder.Decode(buffer, 0, length);
                            Console.WriteLine($"Received query: {query} with transaction ID: {transactionId}");
        
                            ExecutionResult result = qp.ExecuteQuery(query, transactionId);
                            byte[] response = resultEncoder.Encode(result);
        
                            stream.Write(response, 0, response.Length);
                        }
                    }
                }
                catch (ArgumentException)
                {
                    // Send error back to client if I dont't forget
                }
            }
        }
    }
}

