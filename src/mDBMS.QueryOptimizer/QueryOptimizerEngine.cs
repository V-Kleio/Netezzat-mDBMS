using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using System.Text.RegularExpressions;

namespace mDBMS.QueryOptimizer
{
    /// <summary>
    /// Engine utama untuk Query Optimization
    /// Menghasilkan execution plan yang optimal
    /// </summary>
    public class QueryOptimizerEngine : IQueryOptimizer {
        private readonly IStorageManager _storageManager;
        private readonly CostEstimator _costEstimator;
        private readonly QueryPlanCache _planCache;
        private readonly QueryOptimizerOptions _options;

        public QueryOptimizerEngine(IStorageManager storageManager, QueryOptimizerOptions? options = null)
        {
            _storageManager = storageManager;
            _costEstimator = new CostEstimator(storageManager);
            _options = options ?? QueryOptimizerOptions.Default;
            _planCache = new QueryPlanCache(_options.PlanCacheCapacity, _options.PlanCacheTTL);
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
            var cacheKey = QuerySignatureBuilder.Build(query);
            if (_options.EnablePlanCaching && _planCache.TryGet(cacheKey, out var cachedPlan))
            {
                return cachedPlan;
            }
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

            if (_options.EnablePlanCaching)
            {
                _planCache.Set(cacheKey, bestPlan);
            }

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

            // Plan 4: Join-aware Strategy
            var joinPlan = GenerateJoinAwarePlan(query);
            if (joinPlan != null) {
                plans.Add(joinPlan);
            }

            // Plan 5: Order-aware Strategy
            var orderPlan = GenerateOrderAwarePlan(query);
            if (orderPlan != null) {
                plans.Add(orderPlan);
            }

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
                EstimatedCost = 0.0, // Dihitung oleh CostEstimator
                Parameters = new Dictionary<string, object?>
                {
                    ["table"] = query.Table
                }
            });

            if (!string.IsNullOrEmpty(query.WhereClause))
            {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 2,
                    Operation = OperationType.FILTER,
                    Description = $"Apply filter: {query.WhereClause}",
                    Table = query.Table,
                    EstimatedCost = 0.0,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["predicate"] = QualifyPredicate(query.WhereClause!, query.Table)
                    }
                });
            }

            if (query.SelectedColumns.Any()) {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = 3,
                    Operation = OperationType.PROJECTION,
                    Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                    Table = query.Table,
                    EstimatedCost = 0.0,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["columns"] = QualifyColumns(query.SelectedColumns, query.Table).ToList()
                    }
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
                    IndexUsed = whereCols.FirstOrDefault(c => indexedColumns.Contains(c)) ?? orderCols.FirstOrDefault(c => indexedColumns.Contains(c)),
                    Parameters = new Dictionary<string, object?>
                    {
                        ["table"] = query.Table,
                        ["indexColumn"] = QualifyColumn(whereCols.FirstOrDefault(c => indexedColumns.Contains(c)) ?? orderCols.FirstOrDefault(c => indexedColumns.Contains(c)), query.Table),
                        ["predicate"] = string.IsNullOrWhiteSpace(query.WhereClause) ? null : QualifyPredicate(query.WhereClause!, query.Table)
                    }
                });

                if (!string.IsNullOrWhiteSpace(query.WhereClause)) {
                    plan.Steps.Add(new QueryPlanStep {
                        Order = 2,
                        Operation = OperationType.FILTER,
                        Description = $"Apply filter: {query.WhereClause}",
                        Table = query.Table,
                        Parameters = new Dictionary<string, object?>
                        {
                            ["predicate"] = QualifyPredicate(query.WhereClause!, query.Table)
                        }
                    });
                }

                if (query.SelectedColumns.Any()) {
                    plan.Steps.Add(new QueryPlanStep {
                        Order = plan.Steps.Count + 1,
                        Operation = OperationType.PROJECTION,
                        Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                        Table = query.Table,
                        Parameters = new Dictionary<string, object?>
                        {
                            ["columns"] = QualifyColumns(query.SelectedColumns, query.Table).ToList()
                        }
                    });
                }

                if (query.OrderBy != null && query.OrderBy.Any()) {
                    // Jika ada index yang sesuai untuk order by, hindari sort eksplisit
                    if (!hasIndexForOrder) {
                        plan.Steps.Add(new QueryPlanStep {
                            Order = plan.Steps.Count + 1,
                            Operation = OperationType.SORT,
                            Description = $"Sort by: {string.Join(", ", query.OrderBy.Select(o => o.Column + (o.IsAscending ? " ASC" : " DESC")))}",
                            Table = query.Table,
                            Parameters = new Dictionary<string, object?>
                            {
                                ["orderBy"] = query.OrderBy.Select(o => new Dictionary<string, object?>
                                {
                                    ["column"] = QualifyColumn(o.Column, query.Table),
                                    ["ascending"] = o.IsAscending
                                }).ToList()
                            }
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
                    EstimatedCost = 0.0,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["table"] = query.Table,
                        ["predicate"] = QualifyPredicate(query.WhereClause!, query.Table)
                    }
                });
            } else {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 1,
                    Operation = OperationType.TABLE_SCAN,
                    Description = $"Table scan on {query.Table}",
                    Table = query.Table,
                    EstimatedCost = 0.0,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["table"] = query.Table
                    }
                });
            }

            if (query.SelectedColumns.Any()) {
                plan.Steps.Add(new QueryPlanStep {
                    Order = 2,
                    Operation = OperationType.PROJECTION,
                    Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                    Table = query.Table,
                    EstimatedCost = 0.0,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["columns"] = QualifyColumns(query.SelectedColumns, query.Table).ToList()
                    }
                });
            }

            return plan;
        }

        /// <summary>
        /// Generate plan yang memodelkan join execution steps.
        /// </summary>
        private QueryPlan? GenerateJoinAwarePlan(Query query)
        {
            if (query.Joins == null || !query.Joins.Any()) return null;

            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.COST_BASED
            };

            int order = 1;

            plan.Steps.Add(new QueryPlanStep
            {
                Order = order++,
                Operation = OperationType.TABLE_SCAN,
                Description = $"Scan base table {query.Table}",
                Table = query.Table,
                Parameters = new Dictionary<string, object?>
                {
                    ["table"] = query.Table
                }
            });

            foreach (var join in query.Joins)
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = order++,
                    Operation = OperationType.NESTED_LOOP_JOIN,
                    Description = $"Join {join.LeftTable} with {join.RightTable} ON {join.OnCondition}",
                    Table = join.RightTable,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["leftTable"] = join.LeftTable,
                        ["rightTable"] = join.RightTable,
                        ["on"] = join.OnCondition
                    }
                });
            }

            if (!string.IsNullOrWhiteSpace(query.WhereClause))
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = order++,
                    Operation = OperationType.FILTER,
                    Description = $"Apply filter: {query.WhereClause}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["predicate"] = query.WhereClause
                    }
                });
            }

            AppendFinalizationSteps(query, plan, ref order);

            return plan;
        }

        /// <summary>
        /// Generate plan spesifik untuk ORDER BY sehingga optimizer bisa memilih index atau sort.
        /// </summary>
        private QueryPlan? GenerateOrderAwarePlan(Query query)
        {
            if (query.OrderBy == null || !query.OrderBy.Any()) return null;
            if (string.IsNullOrWhiteSpace(query.Table)) return null;

            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.HEURISTIC
            };

            int order = 1;
            bool canUseIndex = false;

            try
            {
                var stats = _storageManager.GetStats(query.Table);
                var indexedColumns = stats.Indices.Select(i => StripTableAlias(i.Item1)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                canUseIndex = query.OrderBy.All(o => indexedColumns.Contains(StripTableAlias(o.Column)));
            }
            catch
            {
                canUseIndex = false;
            }

            plan.Steps.Add(new QueryPlanStep
            {
                Order = order++,
                Operation = canUseIndex ? OperationType.INDEX_SCAN : OperationType.TABLE_SCAN,
                Description = canUseIndex
                    ? $"Index scan on {query.Table} using ORDER BY"
                    : $"Table scan on {query.Table} before sort",
                Table = query.Table,
                Parameters = new Dictionary<string, object?>
                {
                    ["table"] = query.Table
                }
            });

            if (!string.IsNullOrWhiteSpace(query.WhereClause))
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = order++,
                    Operation = OperationType.FILTER,
                    Description = $"Apply filter: {query.WhereClause}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["predicate"] = QualifyPredicate(query.WhereClause!, query.Table)
                    }
                });
            }

            if (!canUseIndex)
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = order++,
                    Operation = OperationType.SORT,
                    Description = $"Sort by: {string.Join(", ", query.OrderBy.Select(o => o.Column + (o.IsAscending ? " ASC" : " DESC")))}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["orderBy"] = query.OrderBy.Select(o => new Dictionary<string, object?>
                        {
                            ["column"] = QualifyColumn(o.Column, query.Table),
                            ["ascending"] = o.IsAscending
                        }).ToList()
                    }
                });
            }

            AppendFinalizationSteps(query, plan, ref order, includeOrderByStep: false);

            return plan;
        }

        private void AppendFinalizationSteps(Query query, QueryPlan plan, ref int nextOrder, bool includeOrderByStep = true)
        {
            if (query.SelectedColumns.Any())
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = nextOrder++,
                    Operation = OperationType.PROJECTION,
                    Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["columns"] = QualifyColumns(query.SelectedColumns, query.Table).ToList()
                    }
                });
            }

            if (query.GroupBy != null && query.GroupBy.Any())
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = nextOrder++,
                    Operation = OperationType.AGGREGATION,
                    Description = $"Group by: {string.Join(", ", query.GroupBy)}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["groupBy"] = QualifyColumns(query.GroupBy, query.Table).ToList()
                    }
                });
            }

            if (includeOrderByStep && query.OrderBy != null && query.OrderBy.Any())
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = nextOrder++,
                    Operation = OperationType.SORT,
                    Description = $"Sort by: {string.Join(", ", query.OrderBy.Select(o => o.Column + (o.IsAscending ? " ASC" : " DESC")))}",
                    Table = query.Table,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["orderBy"] = query.OrderBy.Select(o => new Dictionary<string, object?>
                        {
                            ["column"] = QualifyColumn(o.Column, query.Table),
                            ["ascending"] = o.IsAscending
                        }).ToList()
                    }
                });
            }
        }

        #endregion

        #region Qualification Helpers
        // Pastikan referensi kolom memakai fullname: table.column
        private static string QualifyColumn(string? column, string table)
        {
            if (string.IsNullOrWhiteSpace(column)) return string.Empty;
            if (column.Contains('.')) return column; // sudah qualified
            if (column == "*") return column;
            return string.IsNullOrWhiteSpace(table) ? column : table + "." + column;
        }

        private static IEnumerable<string> QualifyColumns(IEnumerable<string> columns, string table)
        {
            foreach (var c in columns)
            {
                yield return QualifyColumn(c, table);
            }
        }

        private static string QualifyPredicate(string predicate, string defaultTable)
        {
            if (string.IsNullOrWhiteSpace(predicate)) return predicate;

            // Tandai rentang literal string agar tidak termodifikasi
            var literalRanges = new List<(int start, int end)>();
            foreach (Match lm in Regex.Matches(predicate, "'[^']*'"))
            {
                literalRanges.Add((lm.Index, lm.Index + lm.Length));
            }

            bool InLiteral(int index)
            {
                for (int i = 0; i < literalRanges.Count; i++)
                {
                    var r = literalRanges[i];
                    if (index >= r.start && index < r.end) return true;
                }
                return false;
            }

            var keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "AND","OR","NOT","IN","LIKE","BETWEEN","IS","NULL","TRUE","FALSE","ASC","DESC"
            };

            // Ganti identifier yang belum qualified menjadi table.identifier
            string pattern = @"\b([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*)?\b";
            string result = Regex.Replace(predicate, pattern, m =>
            {
                // Lewati jika di dalam literal string
                if (InLiteral(m.Index)) return m.Value;

                var name = m.Groups[1].Value;
                var hasDot = m.Groups[2].Success;

                // Lewati jika keyword atau bernoktah (sudah qualified) atau angka
                if (hasDot) return m.Value;
                if (keywords.Contains(name)) return m.Value;
                if (double.TryParse(m.Value, out _)) return m.Value;

                return QualifyColumn(name, defaultTable);
            });

            return result;
        }
        private static string StripTableAlias(string column)
        {
            if (string.IsNullOrWhiteSpace(column)) return string.Empty;
            var partIdx = column.IndexOf('.');
            return partIdx >= 0 ? column.Substring(partIdx + 1) : column;
        }
        #endregion

        public void ClearPlanCache()
        {
            _planCache.Clear();
        }

        public QueryOptimizerOptions GetOptions()
        {
            return _options;
        }
    }
}
