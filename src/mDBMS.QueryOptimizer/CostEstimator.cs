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
            // TODO: Mendapatkan statistik tabel dan indeks dari storage manager
            // Untuk sekarang, menggunakan basic esimation berdasarkan tipe operasi

            try {
                var stats = _storageManager.GetStats(step.Table);

                return step.Operation switch {
                    OperationType.TABLE_SCAN => EstimateTableScanCost(stats),
                    OperationType.INDEX_SCAN => EstimateIndexScanCost(stats),
                    OperationType.INDEX_SEEK => EstimateIndexSeekCost(stats),
                    OperationType.FILTER => EstimateFilterCost(stats),
                    OperationType.PROJECTION => EstimateProjectionCost(stats),
                    OperationType.SORT => EstimateSortCost(stats),
                    OperationType.NESTED_LOOP_JOIN => EstimateNestedLoopJoinCost(stats),
                    OperationType.HASH_JOIN => EstimateHashJoinCost(stats),
                    OperationType.MERGE_JOIN => EstimateMergeJoinCost(stats),
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
            double indexCost = Math.Log2(stats.TupleCount) * INDEX_SEEK_COST;
            double tupleRetrievalCost = stats.TupleCount * CPU_COST_PER_TUPLE * 0.5;
            return indexCost + tupleRetrievalCost;
        }

        /// <summary>
        /// Estimate cost untuk index seek (selective search)
        /// </summary>
        private double EstimateIndexSeekCost(Statistic stats) {
            // Assume selectivity factor of 0.1 (10% of data retrieved)
            double selectivity = 0.1;
            double indexCost = Math.Log2(stats.TupleCount) * INDEX_SEEK_COST;
            double tupleRetrievalCost = stats.TupleCount * selectivity * CPU_COST_PER_TUPLE;
            return indexCost + tupleRetrievalCost;
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
            if (stats.TupleCount == 0) return 0;

            double sortComplexity = stats.TupleCount * Math.Log2(stats.TupleCount);
            return sortComplexity * CPU_COST_PER_TUPLE * SORT_COST_MULTIPLIER;
        }

        /// <summary>
        /// Estimate cost untuk nested loop join
        /// Complexity = O(n * m) dimana n dan m adalah size dari kedua tabel
        /// </summary>
        private double EstimateNestedLoopJoinCost(Statistic stats) {
            // TODO: Mendapatkan statistik dari kedua tabel yang dijoin
            // Untuk sekarang, memakai asumsi kuadrat dari tuple count
            return stats.TupleCount * stats.TupleCount * CPU_COST_PER_TUPLE;
        }

        /// <summary>
        /// Estimate cost untuk hash join
        /// Complexity = O(n + m)
        /// </summary>
        private double EstimateHashJoinCost(Statistic stats) {
            return stats.TupleCount * CPU_COST_PER_TUPLE * 2;
        }

        /// <summary>
        /// Estimate cost untuk merge join
        /// Complexity = O(n + m) jika data sudah sorted
        /// </summary>
        private double EstimateMergeJoinCost(Statistic stats) {
            return stats.TupleCount * CPU_COST_PER_TUPLE * 1.5;
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
    }
}
