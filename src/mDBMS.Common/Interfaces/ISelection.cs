namespace mDBMS.Common.Interfaces;

public interface ISelection
{
    public void Visit(ISelectionVisitor visitor);
}