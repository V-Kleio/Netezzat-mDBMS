using mDBMS.Common.Interfaces;
namespace mDBMS.Common.Models;

public class ProductNode : ISelection
{
    public ISelection lhs;
    public ISelection rhs;

    public ProductNode(ISelection lhs, ISelection rhs)
    {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public void Visit(ISelectionVisitor visitor)
    {
        visitor.VisitProductNode(this);
    }
}