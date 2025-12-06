using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryProcessor.DML;

public partial class Operator : IPlanNodeVisitor<IEnumerable<Row>>
{
    public IEnumerable<Row> VisitIndexSeekNode(IndexSeekNode node)
    {
        Console.WriteLine($"[INFO] Melakukan Index Seek untuk tabel: {node.TableName}");

        Condition[] seekConditions = node.SeekConditions.ToArray();

        foreach (Condition condition in seekConditions)
        {
            switch (condition.rel)
            {
                case Condition.Relation.COLUMN_AND_VALUE:
                    if (((string) condition.lhs).StartsWith($"{node.TableName}."))
                    {
                        condition.lhs = ((string) condition.lhs).Substring($"{node.TableName}.".Length);
                    }
                break;
                case Condition.Relation.VALUE_AND_COLUMN:
                    if (((string) condition.rhs).StartsWith($"{node.TableName}."))
                    {
                        condition.rhs = ((string) condition.rhs).Substring($"{node.TableName}.".Length);
                    }
                break;
                case Condition.Relation.COLUMN_AND_COLUMN:
                    if (((string) condition.lhs).StartsWith($"{node.TableName}."))
                    {
                        condition.lhs = ((string) condition.lhs).Substring($"{node.TableName}.".Length);
                    }
                    if (((string) condition.rhs).StartsWith($"{node.TableName}."))
                    {
                        condition.rhs = ((string) condition.rhs).Substring($"{node.TableName}.".Length);
                    }
                break;
                default:
                    throw new Exception("unknown condition relation");
            }
        }

        foreach (Row row in storageManager.ReadBlock(new(node.TableName, [])))
        {
            Row canonRow = new() { id = row.id };

            foreach (var (key, val) in row.Columns)
            {
                canonRow[$"{node.TableName}." + key] = val;
            }

            Console.Write($"[INFO] Retrieved row with id: {row.id}");

            yield return canonRow;
        }
    }
}