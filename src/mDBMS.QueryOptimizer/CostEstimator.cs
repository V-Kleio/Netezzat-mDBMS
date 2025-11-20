using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer
{
    /// <summary>
    /// Cost Estimator untuk menghitung estimasi biaya eksekusi query
    /// Menggunakan statistik dari storage manager
    /// </summary>
    public class CostEstimator {
        private readonly IStorageManager _storageManager;

        // Cost constants (dapat dituning berdasarkan kemampuan hardware)
        private const double CPU_COST_PER_TUPLE = 0.01;
        private const double IO_COST_PER_BLOCK = 1.0;
        private const double INDEX_SEEK_COST = 2.0;
        private const double SORT_COST_MULTIPLIER = 1.5;

        public CostEstimator(IStorageManager storageManager) {
            _storageManager = storageManager;
        }

        /// <summary>
        /// Perkiraan cost untuk satu langkah eksekusi
        /// </summary>
        public double EstimateStepCost(QueryPlanStep step, Query query) {
            // Menggunakan statistik nyata dari StorageManager
            // Aku rapihin dikit yogs :v
            try {
                var stats = _storageManager.GetStats(step.Table);
                var selectivity = EstimateSelectivity(query.WhereClause ?? string.Empty, stats);

                return step.Operation switch {
                    OperationType.TABLE_SCAN       => EstimateTableScanCost(stats),
                    OperationType.INDEX_SCAN       => EstimateIndexScanCost(stats),
                    OperationType.INDEX_SEEK       => EstimateIndexSeekCost(stats, selectivity),
                    OperationType.FILTER           => EstimateFilterCost(stats),
                    OperationType.PROJECTION       => EstimateProjectionCost(stats),
                    OperationType.SORT             => EstimateSortCost(stats),
                    OperationType.NESTED_LOOP_JOIN => EstimateNestedLoopJoinCost(stats),
                    OperationType.HASH_JOIN        => EstimateHashJoinCost(stats),
                    OperationType.MERGE_JOIN       => EstimateMergeJoinCost(stats),
                    _ => 100.0 // Default cost
                };
            } catch {
                // If stats not available, return default cost
                return 100.0;
            }
        }

        #region Cost Estimation Methods

        /// <summary>
        /// Estimate cost untuk full table scan
        /// Cost = (Banyaknya Blocks * IO Cost) + (Banyaknya Tuples * CPU Cost)
        /// </summary>
        private double EstimateTableScanCost(Statistic stats) {
            double ioCost = stats.BlockCount * IO_COST_PER_BLOCK;
            double cpuCost = stats.TupleCount * CPU_COST_PER_TUPLE;
            return ioCost + cpuCost;
        }

        /// <summary>
        /// Estimate cost untuk index scan
        /// </summary>
        private double EstimateIndexScanCost(Statistic stats) {
            // Index scan lebih efisien dari table scan
            // Aku ubah dikit yogs :v (rhio)
            int tuples = Math.Max(stats.TupleCount, 0);
            int blocks = Math.Max(stats.BlockCount, 0);
            double traversal = SafeLog2(Math.Max(tuples, 1)) * INDEX_SEEK_COST; // traversal B-Tree (SafeLog2 ada dibawah)
            double ioCost = Math.Ceiling(blocks * 0.3) * IO_COST_PER_BLOCK;     // asumsi 30% blok dibaca
            double cpuCost = tuples * CPU_COST_PER_TUPLE * 0.5;                 // sebagian tuple disentuh
            return traversal + ioCost + cpuCost;
        }

        /// <summary>
        /// Estimate cost untuk index seek (selective search) | 
        /// Selectivity default = 10%
        /// </summary>
        private double EstimateIndexSeekCost(Statistic stats, double selectivity = 0.1) {
            // Index seek: traversal + I/O page fetch + CPU untuk tuple yang terpilih
            int tuples = Math.Max(stats.TupleCount, 0);
            double indexTraversal = SafeLog2(Math.Max(tuples, 1)) * INDEX_SEEK_COST;
            double expectedRows = tuples * Math.Clamp(selectivity, 0.0, 1.0);
            double ioCost = Math.Ceiling(expectedRows / Math.Max(stats.BlockingFactor, 1.0)) * IO_COST_PER_BLOCK;
            double cpuCost = expectedRows * CPU_COST_PER_TUPLE;
            return indexTraversal + ioCost + cpuCost;
        }

        /// <summary>
        /// Estimate cost untuk filter operation
        /// </summary>
        private double EstimateFilterCost(Statistic stats) {
            // Filter cost adalah CPU cost untuk evaluate setiap tuple
            return stats.TupleCount * CPU_COST_PER_TUPLE;
        }

        /// <summary>
        /// Estimate cost untuk projection operation
        /// </summary>
        private double EstimateProjectionCost(Statistic stats) {
            return stats.TupleCount * CPU_COST_PER_TUPLE * 0.1;
        }

        /// <summary>
        /// Estimate cost untuk sort operation
        /// Complexity = O(n log n)
        /// </summary>
        private double EstimateSortCost(Statistic stats) {
            // Aku ubah dikit yogs :v log2(1) kan 0 ye...
            if (stats.TupleCount <= 1) return 0;
            double n = stats.TupleCount;
            double sortComplexity = n * SafeLog2(n);
            return sortComplexity * CPU_COST_PER_TUPLE * SORT_COST_MULTIPLIER;
        }

        /// <summary>
        /// Estimate cost untuk nested loop join
        /// Complexity = O(n * m) dimana n dan m adalah size dari kedua tabel
        /// </summary>
        private double EstimateNestedLoopJoinCost(Statistic stats) {
            // TODO: Mendapatkan statistik dari kedua tabel yang dijoin
            // Untuk sekarang, memakai asumsi kuadrat dari tuple count
            double n = Math.Max(stats.TupleCount, 0);
            return n * n * CPU_COST_PER_TUPLE;
            // return stats.TuplCount * stats.TupleCount * CPU_COST_PER_TUPLE;
        }

        /// <summary>
        /// Estimate cost untuk hash join
        /// Complexity = O(n + m)
        /// </summary>
        private double EstimateHashJoinCost(Statistic stats) {
            double n = Math.Max(stats.TupleCount, 0);
            return n * CPU_COST_PER_TUPLE * 2;
            // return stats.TupleCount * CPU_COST_PER_TUPLE * 2;
        }

        /// <summary>
        /// Estimate cost untuk merge join
        /// Complexity = O(n + m) jika data sudah sorted
        /// </summary>
        private double EstimateMergeJoinCost(Statistic stats) {
            double n = Math.Max(stats.TupleCount, 0);
            return n * CPU_COST_PER_TUPLE * 1.5;
            // return stats.TupleCount * CPU_COST_PER_TUPLE * 1.5;
        }

        #endregion

        /// <summary>
        /// Calculate selectivity factor untuk predicate
        /// </summary>
        public double EstimateSelectivity(string predicate, Statistic stats) {
            // TODO: Perkiraan selectivity yang lebih canggih
            // Untuk sekarang, return selectivity default

            if (string.IsNullOrEmpty(predicate))
                return 1.0; // No filter, semua baris dipilih

            // Heuristik sederhana: equality predicate = 1/distinctValues
            if (predicate.Contains("="))
                return 1.0 / Math.Max(stats.DistinctValues, 1);

            // Range predicate: Asumsi 30% selectivity
            if (predicate.Contains("<") || predicate.Contains(">"))
                return 0.3;

            // Default selectivity
            return 0.5;
        }

        /// <summary>
        /// Safe log2 function supaya gak ada log(0)
        /// </summary>
        private static double SafeLog2(double x)
        {
            if (x <= 1.0) return 0.0;
            return Math.Log2(x);
        }
    }
}
