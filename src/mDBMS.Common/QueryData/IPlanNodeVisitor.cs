namespace mDBMS.Common.QueryData;

public interface IPlanNodeVisitor<R>
{
    public R VisitTableScanNode(TableScanNode node);
    public R VisitIndexScanNode(IndexScanNode node);
    public R VisitIndexSeekNode(IndexSeekNode node);
    public R VisitFilterNode(FilterNode node);
    public R VisitProjectNode(ProjectNode node);
    public R VisitSortNode(SortNode node);
    public R VisitAggregateNode(AggregateNode node);
    public R VisitJoinNode(JoinNode node);
    public R VisitUpdateNode(UpdateNode node);
    public R VisitInsertNode(InsertNode node);
    public R VisitDeleteNode(DeleteNode node);
}

public interface IPlanNodeVisitor
{
    public void VisitTableScanNode(TableScanNode node);
    public void VisitIndexScanNode(IndexScanNode node);
    public void VisitIndexSeekNode(IndexSeekNode node);
    public void VisitFilterNode(FilterNode node);
    public void VisitProjectNode(ProjectNode node);
    public void VisitSortNode(SortNode node);
    public void VisitAggregateNode(AggregateNode node);
    public void VisitJoinNode(JoinNode node);
    public void VisitUpdateNode(UpdateNode node);
    public void VisitInsertNode(InsertNode node);
    public void VisitDeleteNode(DeleteNode node);
}