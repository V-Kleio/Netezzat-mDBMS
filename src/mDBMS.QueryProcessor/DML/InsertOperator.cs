using mDBMS.Common.Data;
using mDBMS.Common.QueryData;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitInsertNode(InsertNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Insert pada tabel: {node.TableName}");

        Dictionary<string, object> newData = [];

        foreach (var (column, value) in node.Columns.Zip(node.Values))
        {
            string localizedColumn = column;

            if (column.StartsWith($"{node.TableName}."))
            {
                localizedColumn = column.Substring($"{node.TableName}.".Length);
            }

            object? parsedValue = value;

            if (int.TryParse(value, out int intres))
            {
                parsedValue = intres;
            }
            else if (float.TryParse(value, out float floatres))
            {
                parsedValue = floatres;
            }

            newData[localizedColumn] = parsedValue;
        }

        storageManager.AddBlock(new(node.TableName, newData));

        Condition[] conditions = new Condition[newData.Count];

        int index = 0;
        foreach (var (column, value) in newData)
        {
            conditions[index] = new()
            {
                lhs = column,
                rhs = value,
                opr = Condition.Operation.EQ,
                rel = Condition.Relation.COLUMN_AND_VALUE
            };

            Console.WriteLine($"       {column} = {value}");

            index++;
        }

        foreach (Row row in storageManager.ReadBlock(new(node.TableName, [], [conditions])))
        {
            // NOTE: CCM validation here is done after insert,
            // This is intentional as it is not possible to lock
            // rows before they exist. The lock operation here is
            // to prevent other transactions from accessing the
            // new row. It should not be possible to fail when
            // trying to attain a lock.
            
            var action = new Common.Transaction.Action(
                Common.Transaction.Action.ActionType.Write,
                new(DatabaseObject.DatabaseObjectType.Row, row.id),
                transactionId
            );

            Response response = concurrencyControlManager.ValidateObject(action);

            if (!response.Allowed)
            {
                throw new Exception($"Write operation ditolak oleh CCM: {response.Reason}");
            }
            
            Row canonizedRow = new() { id = row.id };

            foreach (var (key, val) in row.Columns)
            {
                canonizedRow[$"{node.TableName}." + key] = val;
            }

            failureRecoveryManager.WriteLog(new()
            {
                Operation = ExecutionLog.OperationType.INSERT,
                TransactionId = transactionId,
                TableName = node.TableName,
                RowIdentifier = canonizedRow.id,
                AfterImage = canonizedRow
            });

            yield return canonizedRow;
        }
    }
}