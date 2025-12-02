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

        public QueryOptimizerEngine(IStorageManager storageManager, QueryOptimizerOptions? options = null)
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
            var query = parser.Parser();
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
            // Handle UPDATE bypass
            if (query.Type == QueryType.UPDATE)
            {
                return GenerateUpdatePlan(query);
            }
            else if (query.Type == QueryType.INSERT)
            {
                return GenerateInsertPlan(query);
            }
            else if (query.Type == QueryType.DELETE)
            {
                return GenerateDeletePlan(query);
            }
            // Build plan tree menggunakan PlanBuilder (heuristic-based)
            PlanNode planTree = _planBuilder.BuildPlan(query);

            // Convert tree ke QueryPlan
            var queryPlan = planTree.ToQueryPlan();
            queryPlan.OriginalQuery = query;
            queryPlan.PlanTree = planTree; // Set tree reference

            return queryPlan;
        }

        /// <summary>
        /// Generate query plan untuk UPDATE statement.
        /// </summary>
        private QueryPlan GenerateUpdatePlan(Query query) {
            var plan = new QueryPlan {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = null
            };

            var stats = _storageManager.GetStats(query.Table);
            double selectivity = _costModel.EstimateSelectivity(query.WhereClause ?? "", stats);
            double affectedRows = stats.TupleCount * selectivity;

            // Cek apakah ada index yang bisa digunakan untuk WHERE
            var indexedColumns = stats.Indices.Select(i => i.Item1).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var whereColumns = SqlParserHelpers.ExtractPredicateColumns(query.WhereClause);
            var indexedWhereCol = whereColumns.FirstOrDefault(c => indexedColumns.Contains(c));
            bool useIndex = indexedWhereCol != null && !string.IsNullOrWhiteSpace(query.WhereClause);

            // Cari baris (Scan/Seek)
            plan.Steps.Add(new QueryPlanStep {
                Order = 1,
                Operation = useIndex ? OperationType.INDEX_SEEK : OperationType.TABLE_SCAN,
                Description = useIndex 
                    ? $"Index seek on {query.Table} using index on {indexedWhereCol}"
                    : $"Full table scan on {query.Table}",
                Table = query.Table,
                IndexUsed = indexedWhereCol,
                EstimatedCost = useIndex 
                    ? _costModel.EstimateIndexSeek(stats, selectivity)
                    : _costModel.EstimateTableScan(stats),
                Parameters = new Dictionary<string, object?> { ["predicate"] = query.WhereClause }
            });

            // Qualify column names di UpdateOperations
            var qualifiedUpdates = query.UpdateOperations.ToDictionary(
                kvp => kvp.Key.Contains('.') ? kvp.Key : $"{query.Table}.{kvp.Key}",
                kvp => kvp.Value
            );

            // Update
            double updateCost = _costModel.EstimateUpdate(affectedRows, stats.BlockingFactor);
            plan.Steps.Add(new QueryPlanStep {
                Order = 2,
                Operation = OperationType.UPDATE,
                Description = $"Update {query.UpdateOperations.Count} column(s) in {query.Table}",
                Table = query.Table,
                EstimatedCost = updateCost,
                Parameters = new Dictionary<string, object?> { ["updates"] = qualifiedUpdates }
            });

            plan.TotalEstimatedCost = plan.Steps.Sum(s => s.EstimatedCost);
            plan.EstimatedRows = (int)affectedRows;
            return plan;
        }

        /// <summary>
        /// Generate query plan untuk INSERT statement.
        /// </summary>
        private QueryPlan GenerateInsertPlan(Query query)
        {
            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = null
            };

            var stats = _storageManager.GetStats(query.Table);
            int indexCount = stats.Indices.Count;

            if (query.Type == QueryType.INSERT && query.InsertValues != null)
            {
                // INSERT ... VALUES (direct insert)
                GenerateInsertValuesPlan(query, plan, stats, indexCount);
            }
            else if (query.Type == QueryType.INSERT && query.InsertFromQuery != null)
            {
                // INSERT ... SELECT (complex: optimize SELECT first)
                GenerateInsertSelectPlan(query, plan, stats, indexCount);
            }
            else
            {
                throw new InvalidOperationException("Invalid INSERT query: neither VALUES nor SELECT");
            }

            return plan;
        }

        /// <summary>
        /// Generate query plan untuk DELETE statement.
        /// </summary>
        private QueryPlan GenerateDeletePlan(Query query)
        {
            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = null
            };

            var stats = _storageManager.GetStats(query.Table);
            int indexCount = stats.Indices.Count;

            // Calculate affected rows
            double selectivity = string.IsNullOrWhiteSpace(query.WhereClause)
                ? 1.0 // DELETE tanpa WHERE = delete all rows
                : _costModel.EstimateSelectivity(query.WhereClause, stats);

            double affectedRows = stats.TupleCount * selectivity;

            // Detect jika ada index yang bisa digunakan untuk WHERE
            var indexedColumns = stats.Indices.Select(i => i.Item1).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var whereColumns = SqlParserHelpers.ExtractPredicateColumns(query.WhereClause);
            var indexedWhereCol = whereColumns.FirstOrDefault(c => indexedColumns.Contains(c));
            bool useIndex = indexedWhereCol != null && !string.IsNullOrWhiteSpace(query.WhereClause);

            // Locate rows to delete
            plan.Steps.Add(new QueryPlanStep
            {
                Order = 1,
                Operation = useIndex ? OperationType.INDEX_SEEK : OperationType.TABLE_SCAN,
                Description = useIndex
                    ? $"Index seek on {query.Table} using index on {indexedWhereCol} (estimated {affectedRows:F0} rows)"
                    : $"Full table scan on {query.Table} (estimated {affectedRows:F0} rows)",
                Table = query.Table,
                IndexUsed = indexedWhereCol,
                EstimatedCost = useIndex
                    ? _costModel.EstimateIndexSeek(stats, selectivity)
                    : _costModel.EstimateTableScan(stats),
                Parameters = new Dictionary<string, object?>
                {
                    ["predicate"] = query.WhereClause ?? "ALL ROWS",
                    ["selectivity"] = selectivity
                }
            });

            // Delete rows. Asumsi: tidak ada cascade untuk simplicity (bisa ditambahkan later)
            bool hasCascade = false; // TODO: detect dari storage manager
            double deleteCost = _costModel.EstimateDelete(affectedRows, stats.BlockingFactor, indexCount, hasCascade);

            plan.Steps.Add(new QueryPlanStep
            {
                Order = 2,
                Operation = OperationType.DELETE,
                Description = $"Delete {affectedRows:F0} row(s) from {query.Table}",
                Table = query.Table,
                EstimatedCost = deleteCost,
                Parameters = new Dictionary<string, object?>
                {
                    ["affectedRows"] = affectedRows,
                    ["hasCascade"] = hasCascade
                }
            });

            // Index maintenance (jika ada index)
            if (indexCount > 0)
            {
                var indexNames = stats.Indices.Select(idx => idx.Item1).ToList();
                double indexMaintenanceCost = affectedRows * indexCount * SafeLog2(Math.Max(stats.TupleCount, 1)) * 0.7;

                plan.Steps.Add(new QueryPlanStep
                {
                    Order = 3,
                    Operation = OperationType.INDEX_MAINTENANCE,
                    Description = $"Update {indexCount} index(es): {string.Join(", ", indexNames)}",
                    Table = query.Table,
                    EstimatedCost = indexMaintenanceCost,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["indices"] = indexNames,
                        ["rowCount"] = affectedRows
                    }
                });
            }

            plan.TotalEstimatedCost = plan.Steps.Sum(s => s.EstimatedCost);
            plan.EstimatedRows = (int)affectedRows;

            return plan;
        }

        /// <summary>
        /// Generate plan untuk INSERT ... VALUES.
        /// </summary>
        private void GenerateInsertValuesPlan(Query query, QueryPlan plan, Common.Data.Statistic stats, int indexCount)
        {
            int rowCount = query.InsertValues?.Count ?? 0;
            int columnCount = query.InsertColumns?.Count ?? query.InsertValues![0].Count;

            // Asumsi: ada constraints jika table memiliki index
            bool hasConstraints = indexCount > 0;

            // Validation (conceptual, tidak ada actual I/O)
            plan.Steps.Add(new QueryPlanStep
            {
                Order = 1,
                Operation = OperationType.FILTER, // reuse for validation
                Description = $"Validate {rowCount} row(s) for INSERT into {query.Table}",
                Table = query.Table,
                EstimatedCost = rowCount * CPU_COST_PER_ROW * 0.1, // minimal validation cost
                Parameters = new Dictionary<string, object?>
                {
                    ["rowCount"] = rowCount,
                    ["columnCount"] = columnCount,
                    ["hasConstraints"] = hasConstraints
                }
            });

            // Insert data
            double insertCost = _costModel.EstimateInsert(rowCount, columnCount, indexCount, hasConstraints);
            plan.Steps.Add(new QueryPlanStep
            {
                Order = 2,
                Operation = OperationType.INSERT,
                Description = rowCount > 1
                    ? $"Batch insert {rowCount} rows into {query.Table}"
                    : $"Insert 1 row into {query.Table}",
                Table = query.Table,
                EstimatedCost = insertCost,
                Parameters = new Dictionary<string, object?>
                {
                    ["values"] = query.InsertValues,
                    ["columns"] = query.InsertColumns,
                    ["isBatch"] = rowCount > 1
                }
            });

            // Index maintenance (jika ada index)
            if (indexCount > 0)
            {
                var indexNames = stats.Indices.Select(idx => idx.Item1).ToList();
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = 3,
                    Operation = OperationType.INDEX_MAINTENANCE,
                    Description = $"Update {indexCount} index(es): {string.Join(", ", indexNames)}",
                    Table = query.Table,
                    EstimatedCost = rowCount * indexCount * HASH_BUILD_COST_PER_ROW,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["indices"] = indexNames,
                        ["rowCount"] = rowCount
                    }
                });
            }

            plan.TotalEstimatedCost = plan.Steps.Sum(s => s.EstimatedCost);
            plan.EstimatedRows = rowCount;
        }

        /// <summary>
        /// Generate plan untuk INSERT ... SELECT.
        /// </summary>
        private void GenerateInsertSelectPlan(Query query, QueryPlan plan, Common.Data.Statistic stats, int indexCount)
        {
            // Step 1: Optimize SELECT query
            var selectPlan = OptimizeSelectQuery(query.InsertFromQuery!);
            int estimatedRows = selectPlan.EstimatedRows;

            // Add SELECT steps sebagai sub-plan
            int stepOrder = 1;
            foreach (var selectStep in selectPlan.Steps)
            {
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = stepOrder++,
                    Operation = selectStep.Operation,
                    Description = $"[SELECT] {selectStep.Description}",
                    Table = selectStep.Table,
                    EstimatedCost = selectStep.EstimatedCost,
                    Parameters = selectStep.Parameters
                });
            }

            // Step 2: Insert result set
            int columnCount = query.InsertColumns?.Count ?? query.InsertFromQuery!.SelectedColumns.Count;
            bool hasConstraints = indexCount > 0;
            double insertCost = _costModel.EstimateInsert(estimatedRows, columnCount, indexCount, hasConstraints);

            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.INSERT,
                Description = $"Insert {estimatedRows} row(s) from SELECT result into {query.Table}",
                Table = query.Table,
                EstimatedCost = insertCost,
                Parameters = new Dictionary<string, object?>
                {
                    ["sourceQuery"] = query.InsertFromQuery,
                    ["estimatedRows"] = estimatedRows
                }
            });

            // Step 3: Index maintenance
            if (indexCount > 0)
            {
                var indexNames = stats.Indices.Select(idx => idx.Item1).ToList();
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = stepOrder++,
                    Operation = OperationType.INDEX_MAINTENANCE,
                    Description = $"Update {indexCount} index(es): {string.Join(", ", indexNames)}",
                    Table = query.Table,
                    EstimatedCost = estimatedRows * indexCount * HASH_BUILD_COST_PER_ROW,
                    Parameters = new Dictionary<string, object?>
                    {
                        ["indices"] = indexNames,
                        ["rowCount"] = estimatedRows
                    }
                });
            }

            plan.TotalEstimatedCost = plan.Steps.Sum(s => s.EstimatedCost);
            plan.EstimatedRows = estimatedRows;
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
        private static string StripTableAlias(string column)
        {
            if (string.IsNullOrWhiteSpace(column)) return string.Empty;
            var partIdx = column.IndexOf('.');
            return partIdx >= 0 ? column.Substring(partIdx + 1) : column;
        }
        #endregion
    }
}
