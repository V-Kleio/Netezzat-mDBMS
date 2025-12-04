using mDBMS.Common.Data;
using mDBMS.Common.QueryData;
using mDBMS.Common.Transaction;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitDeleteNode(DeleteNode node)
    {
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
                conditions[index].rel = Condition.Relation.COLUMN_AND_VALUE;
                conditions[index].opr = Condition.Operation.EQ;
                conditions[index].lhs = key;
                conditions[index].rhs = val;

                if (key.StartsWith($"{node.TableName}."))
                {
                    conditions[index].lhs = key.Substring($"{node.TableName}.".Length);
                }

                index++;
            }

            storageManager.DeleteBlock(new(node.TableName, [conditions]));

            failureRecoveryManager.WriteLog(new()
            {
                Operation = ExecutionLog.OperationType.DELETE,
                TransactionId = transactionId,
                TableName = node.TableName,
                RowIdentifier = row.id,
                BeforeImage = row
            });

            yield return row;
        }
    }
}