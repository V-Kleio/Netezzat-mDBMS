using mDBMS.Common.Models;
namespace mDBMS.Common.Interfaces;

public interface ISelectionVisitor
{
    public void VisitSelectNode(SelectNode node);
    public void VisitProjectNode(ProjectNode node);
    public void VisitProductNode(ProductNode node);
    public void VisitJoinNode(JoinNode node);
    public void VisitTableNode(TableNode node);
}