using mDBMS.Common.Interfaces;
namespace mDBMS.Common.Models;

public class SelectNode : ISelection
{
    public IEnumerable<Condition> conditions;
    public ISelection child;

    public SelectNode(ISelection child)
    {
        this.child = child;
        this.conditions = [];
    }

    public SelectNode(ISelection child, IEnumerable<Condition> conditions)
    {
        this.child = child;
        this.conditions = conditions;
    }

    public void Visit(ISelectionVisitor visitor)
    {
        visitor.VisitSelectNode(this);
    }
}