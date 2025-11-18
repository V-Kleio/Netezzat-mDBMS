using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer
{
    /// <summary>
    /// Engine utama untuk Query Optimization
    /// Menghasilkan execution plan yang optimal
    /// </summary>
    public class QueryOptimizerEngine : IQueryOptimizer {
        private readonly IStorageManager _storageManager;
        private readonly CostEstimator _costEstimator;

        public QueryOptimizerEngine(IStorageManager storageManager)
        {
            _storageManager = storageManager;
            _costEstimator = new CostEstimator(storageManager);
        }

        /// <summary>
        /// Melakukan parsing query string yang diberikan oleh user
        /// </summary>
        /// <param name="queryString">String query awal</param>
        /// <returns>Representasi pohon dari query</returns>
        public Query ParseQuery(string queryString)
        {
            Console.WriteLine($"[STUB QO]: ParseQuery dipanggil untuk kueri '{queryString}'");
            return new Query();
        }

        /// <summary>
        /// Mengoptimalkan query dan menghasilkan execution plan yang efisien
        /// </summary>
        /// <param name="query">Query yang akan dioptimalkan</param>
        /// <returns>Optimized query execution plan</returns>
        public QueryPlan OptimizeQuery(Query query) {
            // TODO: Implementasi full query optimization
            // Untuk sekarang, return basic plan

            var plan = new QueryPlan {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.COST_BASED
            };

            // Generate plan alternatif
            var alternativePlans = GenerateAlternativePlans(query);

            // Pilih plan dengan cost termurah
            var bestPlan = alternativePlans
                .OrderBy(p => GetCost(p))
                .FirstOrDefault() ?? plan;

            // Hitung cost akhir
            bestPlan.TotalEstimatedCost = GetCost(bestPlan);

            return bestPlan;
        }

        /// <summary>
        /// Menghitung estimasi cost untuk sebuah query plan
        /// </summary>
        /// <param name="plan">Query plan yang akan dihitung costnya</param>
        /// <returns>Estimasi cost dalam bentuk numerik</returns>
        public double GetCost(QueryPlan plan) {
            // TODO: Implementasi sophisticated cost calculation
            // Rumus: Total Cost = sum(CPU Cost + I/O Cost + Network Cost)

            double totalCost = 0.0;

            foreach (var step in plan.Steps) {
                totalCost += _costEstimator.EstimateStepCost(step, plan.OriginalQuery);
            }

            return totalCost;
        }

        /// <summary>
        /// Menggenerate beberapa alternatif query plan
        /// </summary>
        /// <param name="query">Query yang akan dianalisis</param>
        /// <returns>Daftar alternatif query plan</returns>
        public IEnumerable<QueryPlan> GenerateAlternativePlans(Query query) {
            // TODO: Generate beberapa alternatif rencana eksekusi
            var plans = new List<QueryPlan>();

            // Plan 1: Table Scan Strategy
            plans.Add(GenerateTableScanPlan(query));

            // Plan 2: Index Scan Strategy (jika dapat diterapkan)
            var indexPlan = GenerateIndexScanPlan(query);
            if (indexPlan != null) {
                plans.Add(indexPlan);
            }

            // Plan 3: Filter Pushdown Strategy
            plans.Add(GenerateFilterPushdownPlan(query));

            return plans;
        }

        #region Helper Methods (Private)

        /// <summary>
        /// Generate plan dengan strategi table scan
        /// </summary>
        private QueryPlan GenerateTableScanPlan(Query query) {
            var plan = new QueryPlan {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED
            };

            plan.Steps.Add(new QueryPlanStep {
                Order = 1,
                Operation = OperationType.TABLE_SCAN,
                Description = $"Full table scan on {query.Table}",
                Table = query.Table,
                EstimatedCost = 0.0 // Dihitung oleh CostEstimator
            });

            if (!string.IsNullOrEmpty(query.WhereClause))
            {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 2,
                    Operation = OperationType.FILTER,
                    Description = $"Apply filter: {query.WhereClause}",
                    Table = query.Table,
                    EstimatedCost = 0.0
                });
            }

            if (query.SelectedColumns.Any()) {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = 3,
                    Operation = OperationType.PROJECTION,
                    Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                    Table = query.Table,
                    EstimatedCost = 0.0
                });
            }

            return plan;
        }

        /// <summary>
        /// Generate plan dengan strategi index scan
        /// </summary>
        private QueryPlan? GenerateIndexScanPlan(Query query) {
            // TODO: Memeriksa apakah ada index yang sesuai untuk query
            // Untuk sekarang, return null sebagai placeholder
            return null;
        }

        /// <summary>
        /// Generate plan dengan filter pushdown optimization
        /// </summary>
        private QueryPlan GenerateFilterPushdownPlan(Query query) {
            var plan = new QueryPlan {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.HEURISTIC
            };

            // Push filter ke bawah untuk scan level untuk meningkatkan efisiensi
            if (!string.IsNullOrEmpty(query.WhereClause)) {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 1,
                    Operation = OperationType.INDEX_SEEK,
                    Description = $"Filtered scan on {query.Table} with condition: {query.WhereClause}",
                    Table = query.Table,
                    EstimatedCost = 0.0
                });
            } else {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 1,
                    Operation = OperationType.TABLE_SCAN,
                    Description = $"Table scan on {query.Table}",
                    Table = query.Table,
                    EstimatedCost = 0.0
                });
            }

            if (query.SelectedColumns.Any()) {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 2,
                    Operation = OperationType.PROJECTION,
                    Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                    Table = query.Table,
                    EstimatedCost = 0.0
                });
            }

            return plan;
        }

        #endregion
    }
}
