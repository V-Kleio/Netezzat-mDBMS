using System.Net;
using System.Net.Sockets;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Net;
using mDBMS.Common.Transaction;

namespace nDBMS.CLI;

class ProcessorProxy : IQueryProcessor
{
    private IPEndPoint endpoint;
    private int transactionId;
    private QueryEncoder encoder;
    private ExecutionResultDecoder decoder;

    public ProcessorProxy(IPEndPoint endpoint)
    {
        this.endpoint = endpoint;
        this.transactionId = -1;
        this.encoder = new();
        this.decoder = new();
    }

    public ExecutionResult ExecuteQuery(string query, int _)
    {
        byte[] message = encoder.Encode(query, transactionId);
        ExecutionResult? result;

        byte[] buffer = new byte[128];
        int length = 0;

        try
        {
            using (TcpClient client = new(endpoint.Address.ToString(), endpoint.Port))
            {
                using (NetworkStream stream = client.GetStream())
                {
                    stream.Write(message);
                    stream.Socket.Shutdown(SocketShutdown.Send);
        
                    length += stream.Read(buffer, length, buffer.Length - length);
                    while (length == buffer.Length)
                    {
                        Array.Resize(ref buffer, buffer.Length * 2);
                        length += stream.Read(buffer, length, buffer.Length - length);
                    }
                }
                
                result = decoder.Decode(buffer, 0, length);                
            }
        }
        catch (ArgumentException e)
        {
            result = new()
            {
                Query = query,
                Success = false,
                Message = e.Message
            };
        }

        return result;
    }
}