using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.QueryOptimizer.Core;

namespace mDBMS.QueryOptimizer;

/// <summary>
/// Builder untuk konstruksi plan tree dari Query object.
/// Menggunakan heuristic rules untuk membangun tree yang efisien.
/// 
/// Heuristic rules yang diterapkan:
/// 1. Push selection (filter) sedekat mungkin ke base table
/// 2. Push projection sedekat mungkin ke base table
/// 3. Pilih join order berdasarkan selectivity
/// 4. Pilih join algorithm berdasarkan size dan index availability
/// 5. Lakukan sort di akhir jika memungkinkan
/// 
/// Principle: Single Responsibility - hanya build tree, tidak calculate cost
/// </summary>
public class PlanBuilder
{
    private readonly IStorageManager _storageManager;
    private readonly ICostModel _costModel;

    public PlanBuilder(IStorageManager storageManager, ICostModel costModel)
    {
        _storageManager = storageManager;
        _costModel = costModel;
    }

    /// <summary>
    /// Build plan tree dari Query object dengan heuristic optimization.
    /// Return: root node dari plan tree.
    /// </summary>
    public PlanNode BuildPlan(Query query)
    {
        // Step 1: Buat base scan node (leaf)
        PlanNode root = BuildBaseScan(query);

        // Step 2: Apply filter jika ada WHERE clause
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            root = BuildFilter(root, query.WhereClause, query);
        }

        // Step 3: Apply projection jika ada SELECT columns (bukan SELECT *)
        if (query.SelectedColumns.Any() && !query.SelectedColumns.Contains("*"))
        {
            root = BuildProjection(root, query.SelectedColumns, query);
        }

        // Step 4: Apply aggregation jika ada GROUP BY
        if (query.GroupBy != null && query.GroupBy.Any())
        {
            root = BuildAggregation(root, query.GroupBy, query);
        }

        // Step 5: Apply sort jika ada ORDER BY
        if (query.OrderBy != null && query.OrderBy.Any())
        {
            root = BuildSort(root, query.OrderBy, query);
        }

        // Step 6: Calculate costs secara bottom-up
        CalculateCosts(root, query);

        return root;
    }

    /// <summary>
    /// Build base scan node (TABLE_SCAN, INDEX_SCAN, atau INDEX_SEEK).
    /// Pilihan tergantung pada ketersediaan index dan WHERE clause.
    /// </summary>
    private PlanNode BuildBaseScan(Query query)
    {
        string tableName = query.Table;
        
        // Coba dapatkan statistik tabel
        try
        {
            var stats = _storageManager.GetStats(tableName);
            
            // Cek apakah ada index yang tersedia
            var indexedColumns = stats.Indices.Select(i => i.Item1).ToHashSet(StringComparer.OrdinalIgnoreCase);
            
            if (indexedColumns.Count == 0)
            {
                // Tidak ada index, gunakan TABLE_SCAN
                return new TableScanNode { TableName = tableName };
            }

            // Ada index, cek apakah bisa digunakan untuk WHERE clause
            if (!string.IsNullOrWhiteSpace(query.WhereClause))
            {
                var whereColumns = SqlParserHelpers.ExtractPredicateColumns(query.WhereClause);
                var indexedWhereCol = whereColumns.FirstOrDefault(c => indexedColumns.Contains(c));
                
                if (indexedWhereCol != null)
                {
                    // Index tersedia untuk WHERE, gunakan INDEX_SEEK
                    return new IndexSeekNode
                    {
                        TableName = tableName,
                        IndexColumn = indexedWhereCol,
                        SeekCondition = query.WhereClause
                    };
                }
            }

            // Index tersedia tapi tidak untuk WHERE, cek ORDER BY
            if (query.OrderBy != null && query.OrderBy.Any())
            {
                var orderColumn = query.OrderBy.First().Column;
                if (indexedColumns.Contains(orderColumn))
                {
                    // Index tersedia untuk ORDER BY, gunakan INDEX_SCAN
                    return new IndexScanNode
                    {
                        TableName = tableName,
                        IndexColumn = orderColumn
                    };
                }
            }

            // Default: TABLE_SCAN
            return new TableScanNode { TableName = tableName };
        }
        catch
        {
            // Jika gagal dapatkan stats, fallback ke TABLE_SCAN
            return new TableScanNode { TableName = tableName };
        }
    }

    /// <summary>
    /// Build filter node untuk WHERE clause.
    /// </summary>
    private PlanNode BuildFilter(PlanNode input, string condition, Query query)
    {
        // Jika input sudah INDEX_SEEK dengan condition yang sama, skip filter
        if (input is IndexSeekNode seekNode && seekNode.SeekCondition == condition)
        {
            return input; // Index seek sudah handle filtering
        }

        return new FilterNode(input, condition);
    }

    /// <summary>
    /// Build projection node untuk SELECT columns.
    /// </summary>
    private PlanNode BuildProjection(PlanNode input, List<string> columns, Query query)
    {
        // Qualify column names dengan table name jika belum
        var qualifiedColumns = columns.Select(c =>
        {
            if (c.Contains('.') || c == "*") return c;
            return $"{query.Table}.{c}";
        }).ToList();

        return new ProjectNode(input, qualifiedColumns);
    }

    /// <summary>
    /// Build aggregation node untuk GROUP BY.
    /// </summary>
    private PlanNode BuildAggregation(PlanNode input, List<string> groupByColumns, Query query)
    {
        return new AggregateNode(input, groupByColumns);
    }

    /// <summary>
    /// Build sort node untuk ORDER BY.
    /// </summary>
    private PlanNode BuildSort(PlanNode input, List<OrderByOperation> orderBy, Query query)
    {
        // Jika input adalah INDEX_SCAN/SEEK yang sudah sorted, skip sort
        if (input is IndexScanNode indexScan && orderBy.Count == 1)
        {
            if (orderBy[0].Column.Equals(indexScan.IndexColumn, StringComparison.OrdinalIgnoreCase) &&
                orderBy[0].IsAscending)
            {
                return input; // Data sudah sorted by index
            }
        }

        return new SortNode(input, orderBy);
    }

    /// <summary>
    /// Calculate costs untuk semua nodes dalam tree secara bottom-up.
    /// </summary>
    private void CalculateCosts(PlanNode node, Query query)
    {
        try
        {
            var stats = _storageManager.GetStats(query.Table);

            switch (node)
            {
                case TableScanNode scanNode:
                    node.EstimatedRows = stats.TupleCount;
                    node.NodeCost = _costModel.EstimateTableScan(stats);
                    break;

                case IndexScanNode indexScanNode:
                    node.EstimatedRows = stats.TupleCount;
                    node.NodeCost = _costModel.EstimateIndexScan(stats);
                    break;

                case IndexSeekNode seekNode:
                    double selectivity = _costModel.EstimateSelectivity(seekNode.SeekCondition, stats);
                    node.EstimatedRows = stats.TupleCount * selectivity;
                    node.NodeCost = _costModel.EstimateIndexSeek(stats, selectivity);
                    break;

                case FilterNode filterNode:
                    CalculateCosts(filterNode.Input, query);
                    double filterSelectivity = _costModel.EstimateSelectivity(filterNode.Condition, stats);
                    node.EstimatedRows = filterNode.Input.EstimatedRows * filterSelectivity;
                    node.NodeCost = _costModel.EstimateFilter(filterNode.Input.EstimatedRows, filterNode.Condition);
                    break;

                case ProjectNode projectNode:
                    CalculateCosts(projectNode.Input, query);
                    node.EstimatedRows = projectNode.Input.EstimatedRows;
                    node.NodeCost = _costModel.EstimateProject(projectNode.Input.EstimatedRows, projectNode.Columns.Count);
                    break;

                case SortNode sortNode:
                    CalculateCosts(sortNode.Input, query);
                    node.EstimatedRows = sortNode.Input.EstimatedRows;
                    node.NodeCost = _costModel.EstimateSort(sortNode.Input.EstimatedRows);
                    break;

                case AggregateNode aggNode:
                    CalculateCosts(aggNode.Input, query);
                    // Estimasi: setelah grouping, jumlah rows berkurang
                    int estimatedGroups = Math.Max(1, (int)(aggNode.Input.EstimatedRows * 0.1)); // asumsi 10% unique groups
                    node.EstimatedRows = estimatedGroups;
                    node.NodeCost = _costModel.EstimateAggregate(aggNode.Input.EstimatedRows, estimatedGroups);
                    break;

                case JoinNode joinNode:
                    CalculateCosts(joinNode.Left, query);
                    CalculateCosts(joinNode.Right, query);
                    // Estimasi join result size (simplified)
                    node.EstimatedRows = joinNode.Left.EstimatedRows * joinNode.Right.EstimatedRows * 0.1; // 10% join selectivity
                    node.NodeCost = joinNode.Algorithm switch
                    {
                        JoinAlgorithm.NestedLoop => _costModel.EstimateNestedLoopJoin(joinNode.Left.EstimatedRows, joinNode.Right.EstimatedRows),
                        JoinAlgorithm.Hash => _costModel.EstimateHashJoin(joinNode.Left.EstimatedRows, joinNode.Right.EstimatedRows),
                        JoinAlgorithm.Merge => _costModel.EstimateMergeJoin(joinNode.Left.EstimatedRows, joinNode.Right.EstimatedRows),
                        _ => 0.0
                    };
                    break;
            }
        }
        catch
        {
            // Jika gagal, set default values
            node.EstimatedRows = 100;
            node.NodeCost = 10.0;
        }
    }
}
