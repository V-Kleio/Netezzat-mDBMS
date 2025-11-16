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
            var lexer = new SqlLexer(queryString);
            var tokens = lexer.Tokenize();
            var parser = new SqlParser(tokens);
            var query = parser.ParseSelect();
            return query;
        }

        /// <summary>
        /// Mengoptimalkan query dan menghasilkan execution plan yang efisien
        /// </summary>
        /// <param name="query">Query yang akan dioptimalkan</param>
        /// <returns>Optimized query execution plan</returns>
        public QueryPlan OptimizeQuery(Query query) {
            // Step 1: Aplikasikan aturan ekuivalensi aljabar relasional (heuristik)
            var rewrittenQuery = QueryRewriter.ApplyHeuristicRules(query);

            // Step 2: Generate plan alternatif berdasarkan query yang sudah di-rewrite
            var alternativePlans = GenerateAlternativePlans(rewrittenQuery).ToList();

            // Step 3: Tambahkan plan berbasis heuristik murni
            var heuristicPlan = HeuristicOptimizer.ApplyHeuristicOptimization(rewrittenQuery, _storageManager);
            alternativePlans.Add(heuristicPlan);

            // Step 4: Pilih plan dengan cost termurah (cost-based optimization)
            var bestPlan = alternativePlans
                .OrderBy(p => GetCost(p))
                .FirstOrDefault();

            if (bestPlan == null)
            {
                // Fallback: buat basic plan
                bestPlan = new QueryPlan {
                    OriginalQuery = query,
                    Strategy = OptimizerStrategy.COST_BASED
                };
            }

            // Step 5: Hitung cost akhir
            bestPlan.TotalEstimatedCost = GetCost(bestPlan);

            return bestPlan;
        }

        /// <summary>
        /// Menghitung estimasi cost untuk sebuah query plan
        /// </summary>
        /// <param name="plan">Query plan yang akan dihitung costnya</param>
        /// <returns>Estimasi cost dalam bentuk numerik</returns>
        public double GetCost(QueryPlan plan) {
            // Rumus dasar: Total Cost = sum(EstimatedStepCost)
            // EstimatedStepCost dihitung oleh CostEstimator menggunakan statistik Storage Manager

            double totalCost = 0.0;

            for (int i = 0; i < plan.Steps.Count; i++) {
                var step = plan.Steps[i];
                var cost = _costEstimator.EstimateStepCost(step, plan.OriginalQuery);
                step.EstimatedCost = cost;
                totalCost += cost;
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
            try {
                if (string.IsNullOrWhiteSpace(query.Table)) return null;

                var stats = _storageManager.GetStats(query.Table);
                var indexedColumns = stats.Indices.Select(i => i.Item1).ToHashSet(StringComparer.OrdinalIgnoreCase);

                if (indexedColumns.Count == 0) return null;

                var whereCols = SqlParserHelpers.ExtractPredicateColumns(query.WhereClause);
                var orderCols = query.OrderBy?.Select(o => o.Column) ?? Enumerable.Empty<string>();

                bool hasIndexForWhere = whereCols.Any(c => indexedColumns.Contains(c));
                bool hasIndexForOrder = orderCols.Any(c => indexedColumns.Contains(c));

                if (!hasIndexForWhere && !hasIndexForOrder) return null;

                var plan = new QueryPlan {
                    OriginalQuery = query,
                    Strategy = OptimizerStrategy.COST_BASED
                };

                // Jika ada predicate yang selektif, gunakan INDEX_SEEK, else INDEX_SCAN
                var useSeek = hasIndexForWhere && !string.IsNullOrWhiteSpace(query.WhereClause);

                plan.Steps.Add(new QueryPlanStep {
                    Order = 1,
                    Operation = useSeek ? OperationType.INDEX_SEEK : OperationType.INDEX_SCAN,
                    Description = useSeek
                        ? $"Index seek on {query.Table} using predicate"
                        : $"Index scan on {query.Table}",
                    Table = query.Table,
                    IndexUsed = whereCols.FirstOrDefault(c => indexedColumns.Contains(c)) ?? orderCols.FirstOrDefault(c => indexedColumns.Contains(c))
                });

                if (!string.IsNullOrWhiteSpace(query.WhereClause)) {
                    plan.Steps.Add(new QueryPlanStep {
                        Order = 2,
                        Operation = OperationType.FILTER,
                        Description = $"Apply filter: {query.WhereClause}",
                        Table = query.Table
                    });
                }

                if (query.SelectedColumns.Any()) {
                    plan.Steps.Add(new QueryPlanStep {
                        Order = plan.Steps.Count + 1,
                        Operation = OperationType.PROJECTION,
                        Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                        Table = query.Table
                    });
                }

                if (query.OrderBy != null && query.OrderBy.Any()) {
                    // Jika ada index yang sesuai untuk order by, hindari sort eksplisit
                    if (!hasIndexForOrder) {
                        plan.Steps.Add(new QueryPlanStep {
                            Order = plan.Steps.Count + 1,
                            Operation = OperationType.SORT,
                            Description = $"Sort by: {string.Join(", ", query.OrderBy.Select(o => o.Column + (o.IsAscending ? " ASC" : " DESC")))}",
                            Table = query.Table
                        });
                    }
                }

                return plan;
            } catch {
                return null;
            }
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
