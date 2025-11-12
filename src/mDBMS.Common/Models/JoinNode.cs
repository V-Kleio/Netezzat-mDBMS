using mDBMS.Common.Interfaces;
namespace mDBMS.Common.Models;

public class JoinNode : ISelection
{
    public ISelection lhs;
    public ISelection rhs;
    public IEnumerable<(string, string)> joinMap;

    public JoinNode(ISelection lhs, ISelection rhs)
    {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinMap = [];
    }

    public JoinNode(ISelection lhs, ISelection rhs, IEnumerable<(string, string)> joinMap)
    {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinMap = joinMap;
    }

    public void Visit(ISelectionVisitor visitor)
    {
        visitor.VisitJoinNode(this);
    }
}