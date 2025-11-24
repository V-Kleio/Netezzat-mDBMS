using mDBMS.Common.Data;
using mDBMS.QueryOptimizer.Core;

namespace mDBMS.QueryOptimizer.Core;

/// <summary>
/// Implementasi cost model berbasis I/O dan CPU cost.
/// Menggunakan konstanta yang dapat di-tune berdasarkan karakteristik hardware.
/// 
/// Formula dasar:
/// - Total Cost = I/O Cost + CPU Cost
/// - I/O Cost = Jumlah Block yang dibaca × Cost per Block
/// - CPU Cost = Jumlah Row yang diproses × Cost per Row
/// 
/// Principle: Single Responsibility - hanya menghitung cost, tidak membuat plan
/// </summary>
public class SimpleCostModel : ICostModel
{
    // === Konstanta Cost (dapat dituning berdasarkan hardware) ===
    
    /// <summary>
    /// Cost untuk memproses satu row (CPU operation).
    /// Mencakup: parsing, evaluation, memory allocation.
    /// </summary>
    private const double CPU_COST_PER_ROW = 0.01;

    /// <summary>
    /// Cost untuk membaca satu block dari disk (I/O operation).
    /// Ini adalah operasi paling mahal, ~100x lebih mahal dari CPU.
    /// </summary>
    private const double IO_COST_PER_BLOCK = 1.0;

    /// <summary>
    /// Base cost untuk tree traversal dalam index seek.
    /// </summary>
    private const double INDEX_SEEK_BASE_COST = 2.0;

    /// <summary>
    /// Multiplier untuk sort operation (karena complexity O(n log n)).
    /// </summary>
    private const double SORT_MULTIPLIER = 1.5;

    /// <summary>
    /// Cost untuk build hash table per row.
    /// </summary>
    private const double HASH_BUILD_COST_PER_ROW = 0.02;

    // === Implementation ICostModel ===

    /// <summary>
    /// Estimasi cost untuk full table scan.
    /// Formula: (Block Count × IO Cost) + (Row Count × CPU Cost)
    /// </summary>
    public double EstimateTableScan(Statistic stats)
    {
        int blockCount = Math.Max(stats.BlockCount, 0);
        int rowCount = Math.Max(stats.TupleCount, 0);

        double ioCost = blockCount * IO_COST_PER_BLOCK;
        double cpuCost = rowCount * CPU_COST_PER_ROW;

        return ioCost + cpuCost;
    }

    /// <summary>
    /// Estimasi cost untuk index scan (scan semua entry di index).
    /// Biasanya lebih efisien dari table scan karena index terorganisir.
    /// Asumsi: hanya 30% block yang perlu dibaca.
    /// </summary>
    public double EstimateIndexScan(Statistic stats)
    {
        int blockCount = Math.Max(stats.BlockCount, 0);
        int rowCount = Math.Max(stats.TupleCount, 0);

        // Index scan: tree traversal + baca sebagian block
        double traversalCost = SafeLog2(Math.Max(rowCount, 1)) * INDEX_SEEK_BASE_COST;
        double ioCost = Math.Ceiling(blockCount * 0.3) * IO_COST_PER_BLOCK;
        double cpuCost = rowCount * CPU_COST_PER_ROW * 0.5; // hanya sebagian row diproses

        return traversalCost + ioCost + cpuCost;
    }

    /// <summary>
    /// Estimasi cost untuk index seek (selective search dengan condition).
    /// Formula: Tree Traversal + I/O untuk row yang match + CPU processing
    /// </summary>
    public double EstimateIndexSeek(Statistic stats, double selectivity)
    {
        int rowCount = Math.Max(stats.TupleCount, 0);
        double blockingFactor = Math.Max(stats.BlockingFactor, 1.0);

        // Clamp selectivity ke range valid [0, 1]
        selectivity = Math.Clamp(selectivity, 0.0, 1.0);

        // Tree traversal cost (B-Tree height)
        double traversalCost = SafeLog2(Math.Max(rowCount, 1)) * INDEX_SEEK_BASE_COST;

        // Expected rows yang match condition
        double expectedRows = rowCount * selectivity;

        // I/O cost: block yang perlu dibaca
        double expectedBlocks = Math.Ceiling(expectedRows / blockingFactor);
        double ioCost = expectedBlocks * IO_COST_PER_BLOCK;

        // CPU cost untuk process matching rows
        double cpuCost = expectedRows * CPU_COST_PER_ROW;

        return traversalCost + ioCost + cpuCost;
    }

    /// <summary>
    /// Estimasi cost untuk filter operation (WHERE clause).
    /// Pure CPU operation, tidak ada I/O.
    /// </summary>
    public double EstimateFilter(double inputRows, string condition)
    {
        // Filter hanya CPU cost untuk evaluate condition pada setiap row
        return inputRows * CPU_COST_PER_ROW;
    }

    /// <summary>
    /// Estimasi cost untuk projection (SELECT columns).
    /// Relatif murah, hanya copy columns.
    /// </summary>
    public double EstimateProject(double inputRows, int columnCount)
    {
        // Projection cost lebih rendah dari filter
        return inputRows * CPU_COST_PER_ROW * 0.1 * columnCount;
    }

    /// <summary>
    /// Estimasi cost untuk sort operation.
    /// Complexity: O(n log n)
    /// </summary>
    public double EstimateSort(double inputRows)
    {
        if (inputRows <= 1) return 0;

        // Sort complexity: n log n
        double sortComplexity = inputRows * SafeLog2(inputRows);
        return sortComplexity * CPU_COST_PER_ROW * SORT_MULTIPLIER;
    }

    /// <summary>
    /// Estimasi cost untuk aggregate (GROUP BY, COUNT, SUM, etc).
    /// Termasuk hashing untuk grouping.
    /// </summary>
    public double EstimateAggregate(double inputRows, int groupCount)
    {
        // Hashing untuk grouping + aggregate computation
        double hashCost = inputRows * HASH_BUILD_COST_PER_ROW;
        double aggregateCost = groupCount * CPU_COST_PER_ROW;
        return hashCost + aggregateCost;
    }

    /// <summary>
    /// Estimasi cost untuk Nested Loop Join.
    /// Complexity: O(n × m) dimana n = outer rows, m = inner rows
    /// Paling lambat tapi paling simple.
    /// </summary>
    public double EstimateNestedLoopJoin(double outerRows, double innerRows)
    {
        // Nested loop: untuk setiap outer row, scan semua inner rows
        return outerRows * innerRows * CPU_COST_PER_ROW;
    }

    /// <summary>
    /// Estimasi cost untuk Hash Join.
    /// Complexity: O(n + m)
    /// Build hash table dari smaller table, probe dari larger table.
    /// </summary>
    public double EstimateHashJoin(double outerRows, double innerRows)
    {
        // Build phase: hash smaller table
        double buildRows = Math.Min(outerRows, innerRows);
        double buildCost = buildRows * HASH_BUILD_COST_PER_ROW;

        // Probe phase: scan larger table dan probe hash table
        double probeRows = Math.Max(outerRows, innerRows);
        double probeCost = probeRows * CPU_COST_PER_ROW;

        return buildCost + probeCost;
    }

    /// <summary>
    /// Estimasi cost untuk Merge Join.
    /// Complexity: O(n + m) jika kedua input sudah sorted
    /// Kalau belum sorted, harus tambah sort cost.
    /// </summary>
    public double EstimateMergeJoin(double outerRows, double innerRows)
    {
        // Merge phase: scan kedua input secara linear
        double mergeCost = (outerRows + innerRows) * CPU_COST_PER_ROW;

        // Asumsi belum sorted, tambahkan sort cost
        double sortCost = EstimateSort(outerRows) + EstimateSort(innerRows);
        return mergeCost + sortCost;
    }

    /// <summary>
    /// Estimasi selectivity untuk condition.
    /// Menentukan berapa persen rows yang akan lolos filter.
    /// 
    /// Heuristic rules:
    /// - No condition: 100% (semua row lolos)
    /// - Equality (=): 1 / distinct_values
    /// - Range (<, >, <=, >=): 30% (conservative estimate)
    /// - LIKE: 10%
    /// - IN: depends on list size
    /// - Default: 10% (conservative)
    /// </summary>
    public double EstimateSelectivity(string condition, Statistic stats)
    {
        if (string.IsNullOrWhiteSpace(condition))
            return 1.0; // No filter, semua row lolos

        // Normalize condition untuk pattern matching
        string normalized = condition.ToLowerInvariant().Trim();

        // Equality predicate: col = value
        if (normalized.Contains("=") && !normalized.Contains("!="))
        {
            // Asumsi uniform distribution
            return 1.0 / Math.Max(stats.DistinctValues, 1);
        }

        // Range predicate: col < value, col > value, BETWEEN
        if (normalized.Contains("<") || normalized.Contains(">") || normalized.Contains("between"))
        {
            return 0.3; // Conservative 30%
        }

        // LIKE predicate (pattern matching)
        if (normalized.Contains("like"))
        {
            return 0.1; // Conservative 10%
        }

        // IN predicate: col IN (val1, val2, ...)
        if (normalized.Contains("in ("))
        {
            // Coba hitung jumlah values dalam IN clause
            var match = System.Text.RegularExpressions.Regex.Match(normalized, @"in\s*\(\s*([^)]+)\)");
            if (match.Success)
            {
                int valueCount = match.Groups[1].Value.Split(',').Length;
                return Math.Min(1.0, valueCount * (1.0 / Math.Max(stats.DistinctValues, 1)));
            }
        }

        // Default: conservative 10%
        return 0.1;
    }

    /// <summary>
    /// Safe logarithm base 2 (menghindari log(0) atau log(1)).
    /// </summary>
    private static double SafeLog2(double x) => x <= 1 ? 0 : Math.Log2(x);
}
