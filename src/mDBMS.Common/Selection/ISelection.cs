namespace mDBMS.Common.Selection;

public interface ISelection
{
    public void Visit(ISelectionVisitor visitor);
}