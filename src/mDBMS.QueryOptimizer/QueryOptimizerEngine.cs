using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.QueryOptimizer.Core;
using System.Text.RegularExpressions;

namespace mDBMS.QueryOptimizer
{
    /// <summary>
    /// Engine utama untuk Query Optimization.
    /// Orchestrator yang mengkoordinasikan parsing, building, dan optimization.
    /// 
    /// Flow:
    /// 1. ParseQuery: SQL string -> Query object (via SqlLexer + SqlParser)
    /// 2. OptimizeQuery: Query object -> QueryPlan dengan PlanNode tree
    /// 3. Return: QueryPlan (dengan PlanTree dan Steps untuk backward compatibility)
    /// 
    /// Principle: Orchestration - delegate ke specialized components
    /// </summary>
    public class QueryOptimizerEngine : IQueryOptimizer {
        private readonly IStorageManager _storageManager;
        private readonly ICostModel _costModel;
        private readonly PlanBuilder _planBuilder;

        public QueryOptimizerEngine(IStorageManager storageManager)
        {
            _storageManager = storageManager;
            _costModel = new SimpleCostModel();
            _planBuilder = new PlanBuilder(storageManager, _costModel);
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
        /// Mengoptimalkan query dan menghasilkan execution plan yang efisien.
        /// 
        /// QueryPlan.PlanTree berisi tree structure.
        /// QueryPlan.Steps berisi flat list (generated dari tree) untuk backward compatibility.
        /// </summary>
        /// <param name="query">Query yang akan dioptimalkan</param>
        /// <returns>Optimized query execution plan dengan tree dan flat steps</returns>
        public QueryPlan OptimizeQuery(Query query) {
            // Build plan tree menggunakan PlanBuilder (heuristic-based)
            PlanNode planTree = _planBuilder.BuildPlan(query);

            // Convert tree ke QueryPlan
            var queryPlan = planTree.ToQueryPlan();
            queryPlan.OriginalQuery = query;
            queryPlan.PlanTree = planTree; // Set tree reference

            return queryPlan;
        }

        /// <summary>
        /// Optimize query dan return PlanNode tree directly.
        /// Ini adalah method untuk direct tree access.
        /// </summary>
        /// <param name="query">Query yang akan dioptimalkan</param>
        /// <returns>Root node dari optimized plan tree</returns>
        public PlanNode OptimizeQueryTree(Query query)
        {
            return _planBuilder.BuildPlan(query);
        }

        /// <summary>
        /// Menghitung estimasi cost untuk sebuah query plan
        /// Cost sudah dihitung saat build tree, method ini hanya return total.
        /// </summary>
        /// <param name="plan">Query plan yang akan dihitung costnya</param>
        /// <returns>Estimasi cost dalam bentuk numerik</returns>
        public double GetCost(QueryPlan plan)
        {
            // Cost sudah calculated di PlanBuilder, tinggal return
            return plan.TotalEstimatedCost;
        }

        /// <summary>
        /// Menggenerate beberapa alternatif query plan
        /// Untuk sekarang hanya return satu plan dari PlanBuilder
        /// TODO: Implement multiple plan alternatives dengan different strategies
        /// </summary>
        /// <param name="query">Query yang akan dianalisis</param>
        /// <returns>Daftar alternatif query plan</returns>
        public IEnumerable<QueryPlan> GenerateAlternativePlans(Query query)
        {
            // Untuk sekarang, hanya return satu optimal plan dari PlanBuilder
            yield return OptimizeQuery(query);
        }
    


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
        #endregion
    }
}
