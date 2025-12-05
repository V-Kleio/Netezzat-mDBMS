using System.Net;
using System.Net.Sockets;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Net;
using mDBMS.Common.Transaction;

namespace mDBMS.CLI;

class ProcessorProxy(IPEndPoint endpoint) : IQueryProcessor
{
    private readonly IPEndPoint endpoint = endpoint;
    private int transactionId = -1;

    public ExecutionResult ExecuteQuery(string query, int txId)
    {
        int effectiveTxId = txId >= 0 ? txId : transactionId;
        byte[] message = QueryEncoder.Encode(query, effectiveTxId);
        ExecutionResult? result;

        byte[] buffer = new byte[128];
        int length = 0;

        try
        {
            using TcpClient client = new(endpoint.Address.ToString(), endpoint.Port);
            using (NetworkStream stream = client.GetStream())
            {
                stream.Write(message);
                stream.Socket.Shutdown(SocketShutdown.Send);

                length += stream.Read(buffer, length, buffer.Length - length);
                if (length == 0)
                {
                    return new()
                    {
                        Query = query,
                        Success = false,
                        Message = "Server closed connection without response"
                    };
                }

                while (length == buffer.Length)
                {
                    Array.Resize(ref buffer, buffer.Length * 2);
                    length += stream.Read(buffer, length, buffer.Length - length);
                }
            }

            result = ExecutionResultDecoder.Decode(buffer, 0, length);

            if (result.TransactionId > 0)
            {
                transactionId = result.TransactionId;
            }
        }
        catch (SocketException e)
        {
            result = new()
            {
                Query = query,
                Success = false,
                Message = $"Connection error: {e.Message}. Is the server running?"
            };
        }
        catch (IOException e)
        {
            result = new()
            {
                Query = query,
                Success = false,
                Message = $"Network error: {e.Message}"
            };
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
        catch (Exception e)
        {
            result = new()
            {
                Query = query,
                Success = false,
                Message = $"Unexpected error: {e.Message}"
            };
        }

        return result;
    }
}
