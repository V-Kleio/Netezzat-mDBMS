using System.Text.RegularExpressions;
using mDBMS.Common.Data;
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
    private readonly Dictionary<string, Statistic> _statsCache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly Regex PredicateColumnRegex = new(@"(?:(?<table>[A-Za-z_][A-Za-z0-9_]*)\.)?(?<column>[A-Za-z_][A-Za-z0-9_]*)\s*(=|<>|!=|>=|<=|>|<)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

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
        // Step 1: Buat base scan node (leaf) dan join subtree jika ada JOIN
        PlanNode root = BuildBaseScan(query.Table, query.WhereClause, query.OrderBy);
        root = BuildJoins(root, query);

        // Step 2: Apply filter jika ada WHERE clause
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            root = BuildFilter(root, query.WhereClause);
        }

        // Step 3: Apply projection jika ada SELECT columns (bukan SELECT *)
        if (query.SelectedColumns.Any() && !query.SelectedColumns.Contains("*"))
        {
            root = BuildProjection(root, query.SelectedColumns, query);
        }

        // Step 4: Apply aggregation jika ada GROUP BY
        if (query.GroupBy != null && query.GroupBy.Any())
        {
            root = BuildAggregation(root, query.GroupBy);
        }

        // Step 5: Apply sort jika ada ORDER BY
        if (query.OrderBy != null && query.OrderBy.Any())
        {
            root = BuildSort(root, query.OrderBy);
        }

        // Step 6: Calculate costs secara bottom-up
        CalculateCosts(root);

        return root;
    }

    /// <summary>
    /// Build base scan node (TABLE_SCAN, INDEX_SCAN, atau INDEX_SEEK).
    /// Pilihan tergantung pada ketersediaan index dan WHERE clause.
    /// </summary>
    private PlanNode BuildBaseScan(string tableName, string? predicate, IEnumerable<OrderByOperation>? orderBy)
    {
        var stats = GetStatsOrDefault(tableName);
        var indexedColumns = new HashSet<string>(stats.Indices.Select(idx => idx.Item1), StringComparer.OrdinalIgnoreCase);
        if (indexedColumns.Count == 0)
        {
            return new TableScanNode {TableName = tableName};
        }

        var predicateIndex = FindIndexedColumnFromPredicate(tableName, indexedColumns, predicate);
        if (!string.IsNullOrWhiteSpace(predicateIndex) && predicateIndex != null)
        {
            return new IndexSeekNode
            {
                TableName = tableName,
                IndexColumn = predicateIndex,
                SeekCondition = predicate!
            };
        }

        var orderIndex = FindIndexedColumnForOrder(tableName, indexedColumns, orderBy);
        if (orderIndex != null)
        {
            return new IndexScanNode
            {
                TableName = tableName,
                IndexColumn = orderIndex
            };
        }
        return new TableScanNode { TableName = tableName };

    }

    private PlanNode BuildJoins(PlanNode currNode, Query query)
    {
        if (query.Joins == null || !query.Joins.Any())
            return currNode;
        foreach (var join in query.Joins)
        {
            var rightNode = BuildBaseScan(join.RightTable, join.OnCondition, null);
            var joinNode = new JoinNode(currNode, rightNode, join.Type, join.OnCondition)
            {
                Algorithm = SelectJoinAlgorithm(currNode, join)
            };
            currNode = joinNode;
        }
        return currNode;
    }

    private JoinAlgorithm SelectJoinAlgorithm(PlanNode leftNode, JoinOperation join)
    {
        var leftTableCand = EnumerateBaseTable(leftNode).FirstOrDefault() ?? join.LeftTable;
        var leftStats = GetStatsOrDefault(leftTableCand);
        var rightStats = GetStatsOrDefault(join.RightTable);

        if (leftStats.TupleCount <= 0 || rightStats.TupleCount <= 0)
            return JoinAlgorithm.NestedLoop;
        if (leftStats.TupleCount < 1000 && rightStats.TupleCount < 1000)
            return JoinAlgorithm.Hash;
        if (leftStats.Indices.Any() && rightStats.Indices.Any())
            return JoinAlgorithm.Merge;
        if (rightStats.Indices.Any())
            return JoinAlgorithm.NestedLoop;
        return JoinAlgorithm.Hash;
    }

    private static string? FindIndexedColumnFromPredicate(string tableName, HashSet<string> indexedColumns, string? predicate)
    {
        if (string.IsNullOrWhiteSpace(predicate))
            return null;
        var matches = PredicateColumnRegex.Matches(predicate);
        foreach (Match match in matches)
        {
            var tableAlias = match.Groups["table"].Success ? match.Groups["table"].Value : null;
            var column = match.Groups["column"].Value;
            var normalizedColumn = NormalizeColumnReference(tableName, tableAlias, column);
            if (normalizedColumn != null && indexedColumns.Contains(normalizedColumn))
            {
                return normalizedColumn;
            }
        }
        foreach (var column in indexedColumns)
        {
            if (predicate.Contains($"{tableName}.{column}", StringComparison.OrdinalIgnoreCase) ||
                predicate.Contains(column, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }
        return null;
    }

    private static string? FindIndexedColumnForOrder(string tableName, HashSet<string> indexedColumns, IEnumerable<OrderByOperation>? orderBy)
    {
        if (orderBy == null)
            return null;
        foreach (var order in orderBy)
        {
            var (tableAlias, column) = SplitColumnReference(order.Column);
            var normalizedColumn = NormalizeColumnReference(tableName, tableAlias, column);
            if (normalizedColumn != null && indexedColumns.Contains(normalizedColumn))
            {
                return normalizedColumn;
            }
        }
        return null;
    }
    private static (string? tableAlias, string column) SplitColumnReference(string columnRef)
    {
        var parts = columnRef.Split('.', 2);
        if (parts.Length == 2)
        {
            return (parts[0], parts[1]);
        }
        return (null, columnRef);
    }

    private static string? NormalizeColumnReference(string targetTable, string? tableAlias, string column)
    {
        if (!string.IsNullOrWhiteSpace(tableAlias))
        {
            return tableAlias.Equals(targetTable, StringComparison.OrdinalIgnoreCase)
                ? column
                : null;
        }
        return column;
    }

    private Statistic GetStatsForNode(PlanNode node)
    {
        var tableNames = EnumerateBaseTable(node);
        // Distinct untuk menghindari duplikasi tabel jika ada join
        // Untuk simplicity, ambil stats dari tabel pertama yang ditemukan
        var firstTable = tableNames.FirstOrDefault();
        return GetStatsOrDefault(firstTable ?? "UnknownTable");
    }
    private Statistic GetStatsOrDefault(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return CreateDefaultStats("UnknownTable");
        if (_statsCache.TryGetValue(tableName, out var cachedStats))
            return cachedStats;
        try
        {
            var stats = _storageManager.GetStats(tableName);
            _statsCache[tableName] = stats;
            return stats;
        }
        catch
        {
            var defaultStats = CreateDefaultStats(tableName);
            _statsCache[tableName] = defaultStats;
            return defaultStats;
        }
    }

    private static Statistic CreateDefaultStats(string tableName)
    {
        return new Statistic
        {
            Table = tableName,
            TupleCount = 1000,
            BlockCount = 100,
            TupleSize = 100,
            BlockingFactor = 10,
            DistinctValues = 100,
            Indices = Array.Empty<(string, IndexType)>()

        };
    }

    private static IEnumerable<string> EnumerateBaseTable(PlanNode node)
    {
        switch (node)
        {
            case TableScanNode scanNode:
                yield return scanNode.TableName;
                break;
            case IndexScanNode indexScanNode:
                yield return indexScanNode.TableName;
                break;
            case IndexSeekNode seekNode:
                yield return seekNode.TableName;
                break;
            case JoinNode joinNode:
                foreach (var table in EnumerateBaseTable(joinNode.Left))
                    yield return table;
                foreach (var table in EnumerateBaseTable(joinNode.Right))
                    yield return table;
                break;
            case FilterNode filterNode:
                foreach (var table in EnumerateBaseTable(filterNode.Input))
                    yield return table;
                break;
            case ProjectNode projectNode:
                foreach (var table in EnumerateBaseTable(projectNode.Input))
                    yield return table;
                break;
            case SortNode sortNode:
                foreach (var table in EnumerateBaseTable(sortNode.Input))
                    yield return table;
                break;
            case AggregateNode aggNode:
                foreach (var table in EnumerateBaseTable(aggNode.Input))
                    yield return table;
                break;

        }
    }

    /// <summary>
    /// Build filter node untuk WHERE clause.
    /// </summary>
    private PlanNode BuildFilter(PlanNode input, string condition)
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
    private PlanNode BuildAggregation(PlanNode input, List<string> groupByColumns)
    {
        return new AggregateNode(input, groupByColumns);
    }

    /// <summary>
    /// Build sort node untuk ORDER BY.
    /// </summary>
    private PlanNode BuildSort(PlanNode input, List<OrderByOperation> orderBy)
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
    private void CalculateCosts(PlanNode node)
    {
        try
        {
            switch (node)
            {
                case TableScanNode scanNode:
                {
                    var stats = GetStatsOrDefault(scanNode.TableName);
                    scanNode.EstimatedRows = Math.Max(1, stats.TupleCount);
                    scanNode.NodeCost = _costModel.EstimateTableScan(stats);
                    break;
                }
                case IndexScanNode indexScanNode:
                {
                    var stats = GetStatsOrDefault(indexScanNode.TableName);
                    indexScanNode.EstimatedRows = Math.Max(1, stats.TupleCount);
                    indexScanNode.NodeCost = _costModel.EstimateIndexScan(stats);
                    break;
                }
                case IndexSeekNode seekNode:
                {
                    var stats = GetStatsOrDefault(seekNode.TableName);
                    var selectivity = _costModel.EstimateSelectivity(seekNode.SeekCondition, stats);
                    seekNode.EstimatedRows = Math.Max(1, stats.TupleCount * selectivity);
                    seekNode.NodeCost = _costModel.EstimateIndexSeek(stats, selectivity);
                    break;
                }
                case FilterNode filterNode:
                {
                    CalculateCosts(filterNode.Input);
                    var stats = GetStatsForNode(filterNode.Input);
                    var selectivity = _costModel.EstimateSelectivity(filterNode.Condition, stats);
                    filterNode.EstimatedRows = Math.Max(1, filterNode.Input.EstimatedRows * selectivity);
                    filterNode.NodeCost = _costModel.EstimateFilter(filterNode.Input.EstimatedRows, filterNode.Condition);
                    break;
                }
                case ProjectNode projectNode:
                {
                    CalculateCosts(projectNode.Input);
                    projectNode.EstimatedRows = projectNode.Input.EstimatedRows;
                    projectNode.NodeCost = _costModel.EstimateProject(projectNode.Input.EstimatedRows, projectNode.Columns.Count);
                    break;
                }
                case SortNode sortNode:
                {
                    CalculateCosts(sortNode.Input);
                    sortNode.EstimatedRows = sortNode.Input.EstimatedRows;
                    sortNode.NodeCost = _costModel.EstimateSort(sortNode.Input.EstimatedRows);
                    break;
                }
                case AggregateNode aggregateNode:
                {
                    CalculateCosts(aggregateNode.Input);
                    var inputRows = Math.Max(1, aggregateNode.Input.EstimatedRows);
                    var estimatedGroups = Math.Max(1, (int)(inputRows * 0.1));
                    aggregateNode.EstimatedRows = estimatedGroups;
                    aggregateNode.NodeCost = _costModel.EstimateAggregate(inputRows, estimatedGroups);
                    break;
                }
                case JoinNode joinNode:
                {
                    CalculateCosts(joinNode.Left);
                    CalculateCosts(joinNode.Right);
                    var leftRows = Math.Max(1, joinNode.Left.EstimatedRows);
                    var rightRows = Math.Max(1, joinNode.Right.EstimatedRows);
                    joinNode.EstimatedRows = Math.Max(1, leftRows * rightRows * 0.1);
                    joinNode.NodeCost = joinNode.Algorithm switch
                    {
                        JoinAlgorithm.NestedLoop => _costModel.EstimateNestedLoopJoin(leftRows, rightRows),
                        JoinAlgorithm.Hash => _costModel.EstimateHashJoin(leftRows, rightRows),
                        JoinAlgorithm.Merge => _costModel.EstimateMergeJoin(leftRows, rightRows),
                        _ => _costModel.EstimateNestedLoopJoin(leftRows, rightRows)
                    };
                    break;
                }
                default:
                    break;
            }
        }   
        catch
        {
            node.EstimatedRows = Math.Max(1, node.EstimatedRows == 0 ? 100 : node.EstimatedRows);
            node.NodeCost = Math.Max(1.0, node.NodeCost);
        }
    }
}
