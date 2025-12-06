using mDBMS.Common.Data;
using mDBMS.Common.QueryData;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitUpdateNode(UpdateNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Update pada tabel {node.TableName}");

        foreach (Row row in node.Input.AcceptVisitor(new Operator(storageManager, failureRecoveryManager, concurrencyControlManager, transactionId)))
        {
            foreach (string rowId in row.id.Split(';'))
            {
                var action = new Common.Transaction.Action(
                    Common.Transaction.Action.ActionType.Write,
                    new(DatabaseObject.DatabaseObjectType.Row, rowId),
                    transactionId
                );

                Response response = concurrencyControlManager.ValidateObject(action);

                if (!response.Allowed)
                {
                    throw new Exception($"Write operation ditolak oleh CCM: {response.Reason}");
                }
            }

            Condition[] conditions = new Condition[row.Columns.Count];

            int index = 0;
            foreach (var (key, val) in row.Columns)
            {
                conditions[index] = new()
                {
                    rel = Condition.Relation.COLUMN_AND_VALUE,
                    opr = Condition.Operation.EQ,
                    lhs = key,
                    rhs = val
                };

                if (key.StartsWith($"{node.TableName}."))
                {
                    conditions[index].lhs = key.Substring($"{node.TableName}.".Length);
                }

                index++;
            }

            Dictionary<string, object> updatedValues = [];

            foreach (var (key, val) in node.UpdateOperations)
            {
                Type valtype = row[key].GetType();
                object? parsedValue = val;
                
                try
                {
                    parsedValue = valtype.GetMethod("Parse")?.Invoke(null, [val]) ?? val;
                }
                catch (Exception)
                {
                }

                string localKey = key;

                if (key.StartsWith($"{node.TableName}."))
                {
                    localKey = key.Substring($"{node.TableName}.".Length);
                }

                updatedValues[localKey] = parsedValue;
            }

            storageManager.WriteBlock(new(node.TableName, updatedValues, [conditions]));

            Row updateRow = new() { id = row.id };

            foreach (var (key, val) in row.Columns)
            {
                string localKey = key;

                if (key.StartsWith($"{node.TableName}."))
                {
                    localKey = key.Substring($"{node.TableName}.".Length);
                }

                if (updatedValues.TryGetValue(localKey, out var newVal))
                {
                    updateRow[key] = newVal;
                }
                else
                {
                    updateRow[key] = val;
                }
            }

            failureRecoveryManager.WriteLog(new()
            {
                Operation = ExecutionLog.OperationType.UPDATE,
                TransactionId = transactionId,
                TableName = node.TableName,
                RowIdentifier = row.id,
                BeforeImage = row,
                AfterImage = updateRow
            });

            yield return updateRow;
        }
    }
}