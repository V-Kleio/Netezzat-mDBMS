using mDBMS.Common.Interfaces;
using mDBMS.Common.Net;
using mDBMS.Common.Transaction;
using mDBMS.ConcurrencyControl;
using mDBMS.FailureRecovery;
using mDBMS.QueryOptimizer;
using mDBMS.QueryProcessor;
using mDBMS.StorageManager;
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

        IStorageManager sm = new StorageEngine();
        IQueryOptimizer qo = new QueryOptimizerEngine(sm);
        IConcurrencyControlManager ccm = new ConcurrencyControlManager();
        IFailureRecoveryManager frm = new FailureRecoveryManager();
        IQueryProcessor qp = new QueryProcessor(sm, qo, ccm, frm);
        QueryDecoder queryDecoder = new QueryDecoder();
        ExecutionResultEncoder resultEncoder = new ExecutionResultEncoder();

        IPEndPoint endpoint = new(IPAddress.Loopback, port);
        TcpListener listener = new(endpoint);

        try
        {
            listener.Start();
            Console.WriteLine($"Server listening on {endpoint}");

            while (true)
            {
                using TcpClient handler = listener.AcceptTcpClient();
                using NetworkStream stream = handler.GetStream();
    
                byte[] buffer = new byte[4096];
                int length = stream.Read(buffer, 0, buffer.Length);

                try
                {
                    var (query, transactionId) = queryDecoder.Decode(buffer, length);

                    Console.WriteLine($"Received query: {query} with transaction ID: {transactionId}");

                    ExecutionResult result = qp.ExecuteQuery(query, transactionId);

                    byte[] response = resultEncoder.Encode(result);

                    stream.Write(response, 0, response.Length);
                }
                catch (ArgumentException)
                {
                    // Send error back to client if I dont't forget
                }
                finally
                {
                    handler.Close();
                }
            }
        }
        finally
        {
            listener.Stop();
        }
    }
}

