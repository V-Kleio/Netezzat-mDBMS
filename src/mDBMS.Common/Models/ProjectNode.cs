using mDBMS.Common.Interfaces;
namespace mDBMS.Common.Models;

public class ProjectNode : ISelection
{
    public IEnumerable<string> columns;
    public ISelection child;

    public ProjectNode(ISelection child)
    {
        this.child = child;
        this.columns = [];
    }

    public ProjectNode(ISelection child, IEnumerable<string> columns)
    {
        this.child = child;
        this.columns = columns;
    }

    public void Visit(ISelectionVisitor visitor)
    {
        visitor.VisitProjectNode(this);
    }
}