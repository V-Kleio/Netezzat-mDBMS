// using mDBMS.Common.Interfaces;
// using mDBMS.Common.Data;
// using System.Text.RegularExpressions;

// namespace mDBMS.QueryOptimizer
// {
//     /// <summary>
//     /// Cost Estimator untuk menghitung estimasi biaya eksekusi query
//     /// Menggunakan statistik dari storage manager
//     /// </summary>
//     public class CostEstimator {
//         private readonly IStorageManager _storageManager;

//         // Cost constants (bisa dituning berdasarkan hardware, sementara pakai nilai default)
//         private const double CPU_COST_PER_TUPLE = 0.01;
//         private const double IO_COST_PER_BLOCK = 1.0;
//         private const double INDEX_SEEK_COST = 2.0;
//         private const double SORT_COST_MULTIPLIER = 1.5;

//         private static readonly Regex EqualityRegex = new(@"=\s*(?<value>'[^']*'|\d+(?:\.\d+)?|[A-Za-z_][A-Za-z0-9_]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
//         private static readonly Regex RangeRegex = new(@"(?<![<>=!])(<|>|<=|>=)", RegexOptions.Compiled);
//         private static readonly Regex BetweenRegex = new(@"\bBETWEEN\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);
//         private static readonly Regex LikePrefixRegex = new(@"\bLIKE\s+'[^']*%'", RegexOptions.Compiled | RegexOptions.IgnoreCase);
//         private static readonly Regex InListRegex = new(@"\bIN\s*\((?<list>[^)]+)\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

//         public CostEstimator(IStorageManager storageManager) {
//             _storageManager = storageManager;
//         }



//         #region Cost Estimation Methods

//         /// <summary>
//         /// Estimate cost untuk full table scan
//         /// Cost = (Banyaknya Blocks * IO Cost) + (Banyaknya Tuples * CPU Cost)
//         /// </summary>
//         private double EstimateTableScanCost(Statistic stats) {
//             double ioCost = stats.BlockCount * IO_COST_PER_BLOCK;
//             double cpuCost = stats.TupleCount * CPU_COST_PER_TUPLE;
//             return ioCost + cpuCost;
//         }

//         /// <summary>
//         /// Estimate cost untuk index scan
//         /// </summary>
//         private double EstimateIndexScanCost(Statistic stats) {
//             // Index scan lebih efisien dari table scan
//             // Aku ubah dikit yogs :v (rhio)
//             int tuples = Math.Max(stats.TupleCount, 0);
//             int blocks = Math.Max(stats.BlockCount, 0);
//             double traversal = SafeLog2(Math.Max(tuples, 1)) * INDEX_SEEK_COST; // traversal B-Tree (SafeLog2 ada dibawah)
//             double ioCost = Math.Ceiling(blocks * 0.3) * IO_COST_PER_BLOCK;     // asumsi 30% blok dibaca
//             double cpuCost = tuples * CPU_COST_PER_TUPLE * 0.5;                 // sebagian tuple disentuh
//             return traversal + ioCost + cpuCost;
//         }

//         /// <summary>
//         /// Estimate cost untuk index seek (selective search) |
//         /// Selectivity default = 10%
//         /// </summary>
//         private double EstimateIndexSeekCost(Statistic stats, double selectivity = 0.1) {
//             // Index seek: traversal + I/O page fetch + CPU untuk tuple yang terpilih
//             int tuples = Math.Max(stats.TupleCount, 0);
//             double indexTraversal = SafeLog2(Math.Max(tuples, 1)) * INDEX_SEEK_COST;
//             double expectedRows = tuples * Math.Clamp(selectivity, 0.0, 1.0);
//             double ioCost = Math.Ceiling(expectedRows / Math.Max(stats.BlockingFactor, 1.0)) * IO_COST_PER_BLOCK;
//             double cpuCost = expectedRows * CPU_COST_PER_TUPLE;
//             return indexTraversal + ioCost + cpuCost;
//         }

//         /// <summary>
//         /// Estimate cost untuk filter operation
//         /// </summary>
//         private double EstimateFilterCost(Statistic stats) {
//             // Filter cost adalah CPU cost untuk evaluate setiap tuple
//             return stats.TupleCount * CPU_COST_PER_TUPLE;
//         }

//         /// <summary>
//         /// Estimate cost untuk projection operation
//         /// </summary>
//         private double EstimateProjectionCost(Statistic stats) {
//             return stats.TupleCount * CPU_COST_PER_TUPLE * 0.1;
//         }

//         /// <summary>
//         /// Estimate cost untuk sort operation
//         /// Complexity = O(n log n)
//         /// </summary>
//         private double EstimateSortCost(Statistic stats) {
//             // Aku ubah dikit yogs :v log2(1) kan 0 ye...
//             if (stats.TupleCount <= 1) return 0;
//             double n = stats.TupleCount;
//             double sortComplexity = n * SafeLog2(n);
//             return sortComplexity * CPU_COST_PER_TUPLE * SORT_COST_MULTIPLIER;
//         }

//         /// <summary>
//         /// Estimate cost untuk nested loop join
//         /// Complexity = O(n * m) dimana n dan m adalah size dari kedua tabel
//         /// </summary>
//         public double EstimateNestedLoopJoinCost(Statistic leftStats, Statistic rightStats) {
//             double leftBlocks = Math.Max(leftStats.BlockCount, 1);
//             double rightBlocks = Math.Max(rightStats.BlockCount, 1);
//             double leftTuples = Math.Max(leftStats.TupleCount, 0);
//             double rightTuples = Math.Max(rightStats.TupleCount, 0);

//             double ioCost = (leftBlocks + (leftBlocks * rightBlocks)) * IO_COST_PER_BLOCK;
//             double cpuCost = leftTuples * rightTuples * CPU_COST_PER_TUPLE;
//             return ioCost + cpuCost;
//         }

//         /// <summary>
//         /// Estimate cost untuk hash join
//         /// Complexity = O(n + m)
//         /// </summary>
//         public double EstimateHashJoinCost(Statistic leftStats, Statistic rightStats) {
//             double leftBlocks = Math.Max(leftStats.BlockCount, 1);
//             double rightBlocks = Math.Max(rightStats.BlockCount, 1);
//             double leftTuples = Math.Max(leftStats.TupleCount, 0);
//             double rightTuples = Math.Max(rightStats.TupleCount, 0);

//             double ioCost = (leftBlocks + rightBlocks) * IO_COST_PER_BLOCK;
//             double cpuCost = (leftTuples + rightTuples) * CPU_COST_PER_TUPLE * 2; // Build and probe phase
//             return ioCost + cpuCost;
//         }

//         /// <summary>
//         /// Estimate cost untuk merge join
//         /// Complexity = O(n + m) jika data sudah sorted
//         /// </summary>
//         public double EstimateMergeJoinCost(Statistic leftStats, Statistic rightStats) {
//             double leftBlocks = Math.Max(leftStats.BlockCount, 1);
//             double rightBlocks = Math.Max(rightStats.BlockCount, 1);
//             double leftTuples = Math.Max(leftStats.TupleCount, 0);
//             double rightTuples = Math.Max(rightStats.TupleCount, 0);

//             double ioCost = (leftBlocks + rightBlocks) * IO_COST_PER_BLOCK;
//             double cpuCost = (leftTuples + rightTuples) * CPU_COST_PER_TUPLE * 1.5; // Sedikit overhead untuk merge
//             return ioCost + cpuCost;
//         }

//         #endregion

//         /// <summary>
//         /// Calculate selectivity factor untuk predicate
//         /// </summary>
//         public double EstimateSelectivity(string predicate, Statistic stats) {
//             if (string.IsNullOrWhiteSpace(predicate))
//             {
//                 return 1.0; // Selektivitas penuh jika tidak ada predicate
//             }

//             var conds = QueryRewriter.SplitConjunctiveConditions(predicate);
//             if (conds.Count == 0)
//             {
//                 conds.Add(predicate);
//             }

//             double selectivity = 1.0;
//             foreach (var cond in conds)
//             {
//                 selectivity *= EstimateConditionSelectivity(cond, stats);
//             }
//             return Math.Clamp(selectivity, 0.01, 1.0);
//         }

//         private double EstimateConditionSelectivity(string condition, Statistic stats)
//         {
//             var normalizedCondition = condition.Trim();
//             // Cek pola kondisi
//             if (string.IsNullOrEmpty(normalizedCondition))
//             {
//                 return 1.0;
//             }
//             if (EqualityRegex.IsMatch(normalizedCondition))
//             {
//                 return Math.Clamp(1.0 / Math.Max(stats.DistinctValues, 1), 0.01, 0.5); // Asumsi 1/distinct values
//             }
//             else if (RangeRegex.IsMatch(normalizedCondition))
//             {
//                 return 0.3; // Asumsi 30% untuk range
//             }
//             else if (BetweenRegex.IsMatch(normalizedCondition))
//             {
//                 return 0.25; // Asumsi 25%
//             }
//             else if (LikePrefixRegex.IsMatch(normalizedCondition))
//             {
//                 return 0.2; // Asumsi 20%
//             }
//             else if (InListRegex.IsMatch(normalizedCondition))
//             {
//                 var match = InListRegex.Match(normalizedCondition);
//                 var listValues = match.Groups["list"].Value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
//                 double fraction = (double)listValues.Length / Math.Max(stats.DistinctValues, 1);
//                 return Math.Clamp(fraction, 0.05, 0.8); // Limit antara 5% sampai 80%
//             }
//             else
//             {
//                 return 0.5; // Default asumsi 50%
//             }
//         }

//         /// <summary>
//         /// Mencoba ambil statistik dari StorageManager
//         /// </summary>
//         private Statistic? TryGetStatsFromSM(string tableName)
//         {
//             if (string.IsNullOrWhiteSpace(tableName))
//             {
//                 return null;
//             }
//             try
//             {
//                 return _storageManager.GetStats(tableName);
//             }
//             catch
//             {
//                 return null;
//             }
//         }

//         private static Statistic CreateDefaultStats(string tableName)
//         {
//             return new Statistic
//             {
//                 Table = tableName,
//                 TupleCount = 1000,
//                 BlockCount = 100,
//                 TupleSize = 100,
//                 BlockingFactor = 100,
//                 DistinctValues = 100
//             };
//         }


//         /// <summary>
//         /// Safe log2 function supaya gak ada log(0)
//         /// </summary>
//         private static double SafeLog2(double x)
//         {
//             if (x <= 1.0) return 0.0;
//             return Math.Log2(x);
//         }
//     }
// }
