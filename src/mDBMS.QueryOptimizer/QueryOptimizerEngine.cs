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

        // === Cost Constants (dari SimpleCostModel untuk penghitungan inline) ===
        private const double CPU_COST_PER_ROW = 0.01;
        private const double HASH_BUILD_COST_PER_ROW = 0.02;

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
            // Validate main table exists (will throw if table not found)
            ValidateTableExists(query.Table);
            
            // Validate JOIN tables if any
            if (query.Joins != null)
            {
                foreach (var join in query.Joins)
                {
                    ValidateTableExists(join.RightTable);
                }
            }

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
        /// Validate that a table exists by attempting to get its statistics.
        /// Throws InvalidOperationException if table does not exist.
        /// </summary>
        private void ValidateTableExists(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new InvalidOperationException("Table name cannot be empty");
            }
            // This will throw if table doesn't exist
            _storageManager.GetStats(tableName);
        }

        /// <summary>
        /// Generate query plan untuk UPDATE statement using PlanNode tree.
        /// </summary>
        private QueryPlan GenerateUpdatePlan(Query query) {
            var stats = _storageManager.GetStats(query.Table);
            double selectivity = _costModel.EstimateSelectivity(query.WhereClause ?? "", stats);
            double affectedRows = stats.TupleCount * selectivity;

            // Cek apakah ada index yang bisa digunakan untuk WHERE
            var indexedColumns = stats.Indices.Select(i => i.Item1).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var whereColumns = SqlParserHelpers.ExtractPredicateColumns(query.WhereClause);
            var indexedWhereCol = whereColumns.FirstOrDefault(c => indexedColumns.Contains(c));
            bool useIndex = indexedWhereCol != null && !string.IsNullOrWhiteSpace(query.WhereClause);

            // Build input scan node
            PlanNode scanNode;
            if (useIndex)
            {
                scanNode = new IndexSeekNode
                {
                    TableName = query.Table,
                    IndexColumn = indexedWhereCol!,
                    NodeCost = _costModel.EstimateIndexSeek(stats, selectivity),
                    EstimatedRows = (int)affectedRows
                };
            }
            else
            {
                scanNode = new TableScanNode
                {
                    TableName = query.Table,
                    NodeCost = _costModel.EstimateTableScan(stats),
                    EstimatedRows = (int)stats.TupleCount
                };
            }

            // Add filter if WHERE clause exists
            if (!string.IsNullOrWhiteSpace(query.WhereClause))
            {
                var conditions = PlanBuilder.ParseConditions(query.WhereClause);
                if (conditions.Any())
                {
                    scanNode = new FilterNode(scanNode, conditions)
                    {
                        NodeCost = affectedRows * CPU_COST_PER_ROW,
                        EstimatedRows = (int)affectedRows
                    };
                }
            }

            // Qualify column names di UpdateOperations
            var qualifiedUpdates = query.UpdateOperations.ToDictionary(
                kvp => kvp.Key.Contains('.') ? kvp.Key : $"{query.Table}.{kvp.Key}",
                kvp => kvp.Value
            );

            // Build UPDATE node
            var updateNode = new UpdateNode(scanNode)
            {
                TableName = query.Table,
                UpdateOperations = qualifiedUpdates,
                NodeCost = _costModel.EstimateUpdate(affectedRows, stats.BlockingFactor),
                EstimatedRows = (int)affectedRows
            };

            var plan = new QueryPlan {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = updateNode,
                TotalEstimatedCost = updateNode.TotalCost,
                EstimatedRows = (int)affectedRows
            };

            return plan;
        }

        /// <summary>
        /// Generate query plan untuk INSERT statement using PlanNode tree.
        /// </summary>
        private QueryPlan GenerateInsertPlan(Query query)
        {
            var stats = _storageManager.GetStats(query.Table);
            int indexCount = stats.Indices.Count();

            PlanNode planTree;
            int estimatedRows;

            if (query.Type == QueryType.INSERT && query.InsertValues != null)
            {
                // INSERT ... VALUES (direct insert)
                int rowCount = query.InsertValues?.Count ?? 0;
                int columnCount = query.InsertColumns?.Count ?? query.InsertValues![0].Count;

                planTree = new InsertNode
                {
                    TableName = query.Table,
                    Columns = query.InsertColumns ?? new List<string>(),
                    Values = query.InsertValues?.SelectMany(v => v).ToList() ?? new List<string>(),
                    NodeCost = _costModel.EstimateInsert(rowCount, columnCount, indexCount, indexCount > 0),
                    EstimatedRows = rowCount
                };
                estimatedRows = rowCount;
            }
            else if (query.Type == QueryType.INSERT && query.InsertFromQuery != null)
            {
                // INSERT ... SELECT (optimize SELECT first)
                var selectPlan = OptimizeSelectQuery(query.InsertFromQuery);
                estimatedRows = selectPlan.EstimatedRows;
                int columnCount = query.InsertColumns?.Count ?? query.InsertFromQuery.SelectedColumns.Count;

                planTree = new InsertNode
                {
                    TableName = query.Table,
                    Columns = query.InsertColumns ?? query.InsertFromQuery.SelectedColumns.ToList(),
                    NodeCost = _costModel.EstimateInsert(estimatedRows, columnCount, indexCount, indexCount > 0),
                    EstimatedRows = estimatedRows
                };
            }
            else
            {
                throw new InvalidOperationException("Invalid INSERT query: neither VALUES nor SELECT");
            }

            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = planTree,
                TotalEstimatedCost = planTree.TotalCost,
                EstimatedRows = estimatedRows
            };

            return plan;
        }

        /// <summary>
        /// Generate query plan untuk DELETE statement using PlanNode tree.
        /// </summary>
        private QueryPlan GenerateDeletePlan(Query query)
        {
            var stats = _storageManager.GetStats(query.Table);

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

            // Build input scan node
            PlanNode scanNode;
            if (useIndex)
            {
                scanNode = new IndexSeekNode
                {
                    TableName = query.Table,
                    IndexColumn = indexedWhereCol!,
                    NodeCost = _costModel.EstimateIndexSeek(stats, selectivity),
                    EstimatedRows = (int)affectedRows
                };
            }
            else
            {
                scanNode = new TableScanNode
                {
                    TableName = query.Table,
                    NodeCost = _costModel.EstimateTableScan(stats),
                    EstimatedRows = (int)stats.TupleCount
                };
            }

            // Add filter if WHERE clause exists
            if (!string.IsNullOrWhiteSpace(query.WhereClause))
            {
                var conditions = PlanBuilder.ParseConditions(query.WhereClause);
                if (conditions.Any())
                {
                    scanNode = new FilterNode(scanNode, conditions)
                    {
                        NodeCost = affectedRows * CPU_COST_PER_ROW,
                        EstimatedRows = (int)affectedRows
                    };
                }
            }

            // Build DELETE node
            int indexCount = stats.Indices.Count();
            var deleteNode = new DeleteNode(scanNode)
            {
                TableName = query.Table,
                NodeCost = _costModel.EstimateDelete(affectedRows, stats.BlockingFactor, indexCount, false),
                EstimatedRows = (int)affectedRows
            };

            var plan = new QueryPlan
            {
                OriginalQuery = query,
                Strategy = OptimizerStrategy.RULE_BASED,
                PlanTree = deleteNode,
                TotalEstimatedCost = deleteNode.TotalCost,
                EstimatedRows = (int)affectedRows
            };

            return plan;
        }

        /// <summary>
        /// Optimasi kueri SELECT dan mengembalikan QueryPlan.
        /// Dipakai secara internal untuk optimasi INSERT...SELECT.
        /// </summary>
        private QueryPlan OptimizeSelectQuery(Query selectQuery)
        {
            // Build plan tree for SELECT query
            PlanNode planTree = _planBuilder.BuildPlan(selectQuery);
            
            // Convert to QueryPlan
            var queryPlan = planTree.ToQueryPlan();
            queryPlan.OriginalQuery = selectQuery;
            queryPlan.PlanTree = planTree;
            
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
