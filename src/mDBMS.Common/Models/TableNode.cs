using mDBMS.Common.Interfaces;
namespace mDBMS.Common.Models;

public class TableNode : ISelection
{
    public string tablename;

    public TableNode(string tablename)
    {
        this.tablename = tablename;
    }

    public void Visit(ISelectionVisitor visitor)
    {
        visitor.VisitTableNode(this);
    }
}