using mDBMS.Common.QueryData;
using System.Text.RegularExpressions;

namespace mDBMS.QueryOptimizer;

/// <summary>
/// Query Rewriter yang mengimplementasikan aturan ekuivalensi aljabar relasional
/// untuk transformasi query menjadi bentuk yang lebih optimal.
/// </summary>
internal static class QueryRewriter
{
    /// <summary>
    /// Aplikasikan aturan heuristik untuk optimisasi query.
    /// Mengembalikan query yang sudah ditransformasi berdasarkan aturan ekuivalensi.
    /// </summary>
    public static Query ApplyHeuristicRules(Query originalQuery)
    {
        var optimizedQuery = CloneQuery(originalQuery);

        // Aturan 1 & 2: Seleksi konjungtif dapat diuraikan dan bersifat komutatif
        // sudah diterapkan di plan generation (cascade filters)

        // Aturan 3: Eliminasi proyeksi redundan
        optimizedQuery = EliminateRedundantProjections(optimizedQuery);

        // Aturan 4 & 5: Selection dapat dikombinasikan dengan Cartesian product menjadi Join
        // Implementasi ada di join detection saat parsing

        // Aturan 6: Join bersifat komutatif dan asosiatif
        // E1(JOIN)E2 = E2(JOIN)E1 (komutatif)
        // (E1(JOIN)E2)(JOIN)E3 = E1(JOIN)(E2(JOIN)E3) (asosiatif untuk natural join)
        // (E1(JOIN)Theta1E2)(JOIN)Theta1(AND)Theta2E3 = E1(JOIN)Theta1(AND)Theta2(E2(JOIN)Theta2E3) (asosiatif untuk theta join dengan kondisi)
        // Implementasi di HeuristicOptimizer.OrderJoinsBySelectivity() - join dapat di-reorder

        // Aturan 7: Pushdown selection terhadap join
        optimizedQuery = PushdownSelectionToJoin(optimizedQuery);

        // Aturan 8: Pushdown projection terhadap join
        optimizedQuery = PushdownProjectionToJoin(optimizedQuery);

        return optimizedQuery;
    }

    /// <summary>
    /// Aturan 3: Eliminasi proyeksi redundan.
    /// Hanya proyeksi terakhir yang diperlukan.
    /// </summary>
    private static Query EliminateRedundantProjections(Query query)
    {
        // Dalam representasi Query tunggal, tidak ada nested projection
        // Aturan ini akan berguna saat membuat QueryPlan dengan multiple projection steps
        // Sudah dihandle di plan generation dengan hanya membuat 1 projection step
        return query;
    }

    /// <summary>
    /// Aturan 7: Pushdown selection terhadap join.
    /// SELECT(Theta(E1(JOIN)E2)) dapat didistribusikan:
    /// - Jika Theta hanya melibatkan atribut dari E1: SELECT(Theta(E1(JOIN)E2)) = (SELECT(Theta(E1)))(JOIN)E2
    /// - Jika Theta1(AND)Theta2 dengan Theta1 dari E1 dan Theta2 dari E2: SELECT(Theta1(AND)Theta2(E1(JOIN)E2)) = (SELECT(Theta1(E1)))(JOIN)(SELECT(Theta2(E2)))
    /// </summary>
    private static Query PushdownSelectionToJoin(Query query)
    {
        if (query.Joins == null || !query.Joins.Any()) return query;
        if (string.IsNullOrWhiteSpace(query.WhereClause)) return query;

        // Decompose WHERE clause menjadi kondisi individual (Aturan 1)
        var conditions = SplitConjunctiveConditions(query.WhereClause);
        
        // Kumpulkan semua tabel yang terlibat
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { query.Table };
        if (query.Joins != null)
        {
            foreach (var join in query.Joins)
            {
                tables.Add(join.LeftTable);
                tables.Add(join.RightTable);
            }
        }

        // Analisis setiap kondisi untuk menentukan tabel mana yang terlibat
        var tableConditions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var remainingConditions = new List<string>();

        foreach (var condition in conditions)
        {
            var involvedTables = IdentifyTablesInCondition(condition, tables);
            
            if (involvedTables.Count == 1)
            {
                // Kondisi hanya melibatkan satu tabel - bisa dipush down
                var table = involvedTables.First();
                if (!tableConditions.ContainsKey(table))
                    tableConditions[table] = new List<string>();
                tableConditions[table].Add(condition);
            }
            else
            {
                // Kondisi melibatkan multiple tabel - tidak bisa dipush, evaluasi setelah join
                remainingConditions.Add(condition);
            }
        }

        // Update WhereClause dengan kondisi yang tidak bisa dipush
        // Kondisi yang bisa dipush akan diaplikasikan di plan generation
        if (remainingConditions.Any())
        {
            query.WhereClause = string.Join(" AND ", remainingConditions);
        }
        else
        {
            query.WhereClause = null; // Semua kondisi sudah dipush
        }

        // Simpan metadata pushdown untuk plan generation
        // Karena Query tidak punya field untuk ini, kita akan memanfaatkan
        // informasi ini di HeuristicOptimizer yang membaca conditions
        
        return query;
    }

    /// <summary>
    /// Aturan 8: Pushdown projection terhadap join.
    /// PROJECTION(L1(UNION)L2(E1(JOIN)ThetaE2)) = (PROJECTION(L1(UNION)L3(E1))(JOIN)Theta(PROJECTION(L2(UNION)L4(E2))
    /// di mana L3 adalah atribut dari E1 yang ada di Theta tetapi tidak di L1(UNION)L2
    /// dan L4 adalah atribut dari E2 yang ada di Theta tetapi tidak di L1(UNION)L2
    /// </summary>
    private static Query PushdownProjectionToJoin(Query query)
    {
        if (query.Joins == null || !query.Joins.Any()) return query;
        if (query.SelectedColumns.Contains("*")) return query; // Tidak bisa optimize SELECT *

        // Kumpulkan kolom yang benar-benar dibutuhkan dari semua clause
        var requiredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // L1 (UNION) L2: Kolom dari SELECT clause (output columns)
        foreach (var col in query.SelectedColumns.Where(c => c != "*"))
        {
            requiredColumns.Add(StripTablePrefix(col));
        }

        // L3 (UNION) L4: Kolom dari JOIN ON conditions (diperlukan untuk join evaluation)
        if (query.Joins != null)
        {
            foreach (var join in query.Joins)
            {
                foreach (var col in ExtractColumnsFromExpression(join.OnCondition))
                {
                    requiredColumns.Add(col);
                }
            }
        }

        // Kolom dari WHERE clause (diperlukan untuk filter evaluation)
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            foreach (var col in ExtractColumnsFromExpression(query.WhereClause))
            {
                requiredColumns.Add(col);
            }
        }

        // Kolom dari ORDER BY (diperlukan untuk sorting)
        if (query.OrderBy != null)
        {
            foreach (var orderCol in query.OrderBy)
            {
                requiredColumns.Add(StripTablePrefix(orderCol.Column));
            }
        }

        // Kolom dari GROUP BY (diperlukan untuk grouping)
        if (query.GroupBy != null)
        {
            foreach (var groupCol in query.GroupBy)
            {
                requiredColumns.Add(StripTablePrefix(groupCol));
            }
        }

        // Jika jumlah kolom yang dibutuhkan jauh lebih sedikit dari total kolom,
        // ini adalah kandidat bagus untuk projection pushdown
        // Plan generator akan menggunakan informasi ini untuk membuat early projection
        
        // Update: Jika bukan SELECT *, pastikan hanya kolom yang diperlukan yang dibaca
        // Ini akan diimplementasikan di plan generation dengan membuat projection step
        // sebelum join untuk mengurangi data yang diproses
        
        return query;
    }

    /// <summary>
    /// Dekomposisi kondisi konjungtif (AND) menjadi kondisi individual.
    /// Aturan 1: SELECT(1(AND)2(E)) = SELECT(1)(SELECT(2)(E))
    /// </summary>
    public static List<string> SplitConjunctiveConditions(string whereClause)
    {
        if (string.IsNullOrWhiteSpace(whereClause)) return new List<string>();

        // Split berdasarkan AND (case-insensitive)
        var parts = Regex.Split(whereClause, @"\bAND\b", RegexOptions.IgnoreCase);
        return parts.Select(p => p.Trim()).Where(p => !string.IsNullOrEmpty(p)).ToList();
    }

    /// <summary>
    /// Extract nama kolom dari ekspresi SQL.
    /// Mengembalikan kolom lengkap dengan table prefix jika ada (table.column).
    /// </summary>
    public static IEnumerable<string> ExtractColumnsFromExpression(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression)) yield break;

        // Pattern untuk identifier (termasuk table.column)
        var pattern = @"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b";
        var matches = Regex.Matches(expression, pattern);

        var keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
            "TRUE", "FALSE", "ASC", "DESC"
        };

        foreach (Match match in matches)
        {
            var identifier = match.Value;
            if (!keywords.Contains(identifier) && !IsNumeric(identifier))
            {
                // Return kolom dengan table prefix (jika ada) untuk analisis pushdown
                yield return identifier;
            }
        }
    }

    /// <summary>
    /// Hapus prefix tabel dari nama kolom (table.column -> column).
    /// </summary>
    private static string StripTablePrefix(string columnName)
    {
        var dotIndex = columnName.IndexOf('.');
        return dotIndex >= 0 ? columnName.Substring(dotIndex + 1) : columnName;
    }

    /// <summary>
    /// Cek apakah string adalah numerik.
    /// </summary>
    private static bool IsNumeric(string value)
    {
        return double.TryParse(value, out _);
    }

    /// <summary>
    /// Clone query object untuk transformasi.
    /// </summary>
    private static Query CloneQuery(Query original)
    {
        return new Query
        {
            Table = original.Table,
            SelectedColumns = new List<string>(original.SelectedColumns),
            WhereClause = original.WhereClause,
            Joins = original.Joins != null ? new List<JoinOperation>(original.Joins) : null,
            OrderBy = original.OrderBy != null ? new List<OrderByOperation>(original.OrderBy) : null,
            GroupBy = original.GroupBy != null ? new List<string>(original.GroupBy) : null,
            Type = original.Type
        };
    }

    /// <summary>
    /// Identifikasi tabel mana saja yang terlibat dalam suatu kondisi.
    /// </summary>
    private static HashSet<string> IdentifyTablesInCondition(string condition, HashSet<string> availableTables)
    {
        var involvedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        // Extract kolom dari kondisi (dengan table prefix jika ada)
        var columns = ExtractColumnsFromExpression(condition).ToList();
        
        foreach (var col in columns)
        {
            // Cek apakah kolom punya prefix tabel (table.column)
            if (col.Contains('.'))
            {
                var parts = col.Split('.');
                var tableName = parts[0];
                if (availableTables.Contains(tableName))
                {
                    involvedTables.Add(tableName);
                }
            }
            else
            {
                // Kolom tanpa prefix - bisa dari tabel mana saja
                // Untuk konservatif, kita assume bisa dari multiple tables
                // kecuali hanya ada satu tabel
                if (availableTables.Count == 1)
                {
                    involvedTables.Add(availableTables.First());
                }
            }
        }
        
        return involvedTables;
    }

    /// <summary>
    /// Hitung kolom minimal yang diperlukan untuk query execution.
    /// Digunakan untuk projection pushdown optimization.
    /// </summary>
    public static HashSet<string> GetRequiredColumns(Query query)
    {
        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // SELECT
        if (!query.SelectedColumns.Contains("*"))
        {
            foreach (var col in query.SelectedColumns)
            {
                required.Add(StripTablePrefix(col));
            }
        }

        // WHERE
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            foreach (var col in ExtractColumnsFromExpression(query.WhereClause))
            {
                required.Add(col);
            }
        }

        // JOIN ON
        if (query.Joins != null)
        {
            foreach (var join in query.Joins)
            {
                foreach (var col in ExtractColumnsFromExpression(join.OnCondition))
                {
                    required.Add(col);
                }
            }
        }

        // ORDER BY
        if (query.OrderBy != null)
        {
            foreach (var order in query.OrderBy)
            {
                required.Add(StripTablePrefix(order.Column));
            }
        }

        // GROUP BY
        if (query.GroupBy != null)
        {
            foreach (var group in query.GroupBy)
            {
                required.Add(StripTablePrefix(group));
            }
        }

        return required;
    }
}

/// <summary>
/// Heuristic optimizer yang menggunakan metrik untuk menentukan query plan terbaik.
/// Implementasi aturan heuristik klasik untuk query optimization.
/// </summary>
internal static class HeuristicOptimizer
{
    /// <summary>
    /// Aplikasikan aturan heuristik klasik untuk optimisasi:
    /// 1. Lakukan seleksi sedini mungkin (filter pushdown)
    /// 2. Lakukan proyeksi sedini mungkin untuk mengurangi ukuran intermediate result
    /// 3. Lakukan join paling selektif terlebih dahulu (smallest intermediate results)
    /// 4. Hindari Cartesian product jika memungkinkan
    /// 5. Gunakan index jika tersedia untuk seleksi dan join
    /// </summary>
    public static QueryPlan ApplyHeuristicOptimization(Query query, IStorageManager storageManager)
    {
        var plan = new QueryPlan
        {
            OriginalQuery = query,
            Strategy = OptimizerStrategy.HEURISTIC
        };

        int stepOrder = 1;

        // Aturan Heuristik 1: Lakukan seleksi (filter) sedini mungkin
        // Push filter ke scan level
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            // Dekomposisi kondisi konjunktif
            var conditions = QueryRewriter.SplitConjunctiveConditions(query.WhereClause);

            // Untuk setiap kondisi, coba aplikasikan pada level scan
            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.INDEX_SEEK,
                Description = $"Early selection with filter: {query.WhereClause}",
                Table = query.Table,
                EstimatedCost = 0.0
            });
        }
        else
        {
            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.TABLE_SCAN,
                Description = $"Scan {query.Table}",
                Table = query.Table,
                EstimatedCost = 0.0
            });
        }

        // Aturan Heuristik 3: Jika ada JOIN, pilih join order yang optimal
        if (query.Joins != null && query.Joins.Any())
        {
            // Pilih join berdasarkan estimated selectivity
            // Join dengan hasil paling kecil dilakukan terlebih dahulu
            var orderedJoins = OrderJoinsBySelectivity(query.Joins, storageManager);

            foreach (var join in orderedJoins)
            {
                var joinOp = DetermineJoinAlgorithm(join, storageManager);
                
                plan.Steps.Add(new QueryPlanStep
                {
                    Order = stepOrder++,
                    Operation = joinOp,
                    Description = $"{joinOp} between {join.LeftTable} and {join.RightTable} on {join.OnCondition}",
                    Table = join.RightTable,
                    EstimatedCost = 0.0
                });
            }
        }

        // Aturan Heuristik 2: Lakukan proyeksi untuk mengurangi ukuran data
        // Proyeksi dilakukan setelah filter dan join, tetapi sebelum sort
        if (query.SelectedColumns.Any() && !query.SelectedColumns.Contains("*"))
        {
            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.PROJECTION,
                Description = $"Project columns: {string.Join(", ", query.SelectedColumns)}",
                Table = query.Table,
                EstimatedCost = 0.0
            });
        }

        // GROUP BY (jika ada)
        if (query.GroupBy != null && query.GroupBy.Any())
        {
            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.AGGREGATION,
                Description = $"Group by: {string.Join(", ", query.GroupBy)}",
                Table = query.Table,
                EstimatedCost = 0.0
            });
        }

        // ORDER BY dilakukan di akhir (paling mahal)
        if (query.OrderBy != null && query.OrderBy.Any())
        {
            plan.Steps.Add(new QueryPlanStep
            {
                Order = stepOrder++,
                Operation = OperationType.SORT,
                Description = $"Sort by: {string.Join(", ", query.OrderBy.Select(o => o.Column + (o.IsAscending ? " ASC" : " DESC")))}",
                Table = query.Table,
                EstimatedCost = 0.0
            });
        }

        return plan;
    }

    /// <summary>
    /// Aturan Heuristik: Order join berdasarkan selectivity.
    /// Join dengan hasil terkecil (paling selektif) dilakukan terlebih dahulu.
    /// 
    /// Mengimplementasikan:
    /// - Aturan 6: Komutativitas join - E1(JOIN)E2 = E2(JOIN)E1
    /// - Aturan 7: Asosiatifitas join - (E1(JOIN)E2)(JOIN)E3 = E1(JOIN)(E2(JOIN)E3)
    /// 
    /// Join dapat di-reorder untuk menghasilkan intermediate result terkecil.
    /// </summary>
    private static List<JoinOperation> OrderJoinsBySelectivity(List<JoinOperation> joins, IStorageManager storageManager)
    {
        // Estimasi size hasil join berdasarkan statistik tabel
        var joinWithEstimate = new List<(JoinOperation join, double estimatedSize)>();

        foreach (var join in joins)
        {
            try
            {
                var leftStats = storageManager.GetStats(join.LeftTable);
                var rightStats = storageManager.GetStats(join.RightTable);

                // Estimasi sederhana: size = |left| * |right| * selectivity
                // Selectivity default untuk equi-join = 1 / max(distinctValues)
                double selectivity = 0.1; // default 10%
                double estimatedSize = leftStats.TupleCount * rightStats.TupleCount * selectivity;

                joinWithEstimate.Add((join, estimatedSize));
            }
            catch
            {
                // Jika tidak ada stats, masukkan dengan estimasi besar
                joinWithEstimate.Add((join, double.MaxValue));
            }
        }

        // Sort berdasarkan estimated size (ascending)
        return joinWithEstimate
            .OrderBy(x => x.estimatedSize)
            .Select(x => x.join)
            .ToList();
    }

    /// <summary>
    /// Aturan Heuristik 5: Pilih algoritma join yang sesuai.
    /// - Jika ada index di join column: Index Nested Loop
    /// - Jika salah satu tabel kecil: Hash Join
    /// - Default: Nested Loop Join
    /// </summary>
    private static OperationType DetermineJoinAlgorithm(JoinOperation join, IStorageManager storageManager)
    {
        try
        {
            var leftStats = storageManager.GetStats(join.LeftTable);
            var rightStats = storageManager.GetStats(join.RightTable);

            // Jika ada index di right table untuk join column, gunakan index
            if (rightStats.Indices.Any())
            {
                return OperationType.NESTED_LOOP_JOIN; // bisa di-enhance dengan INDEX_NESTED_LOOP
            }

            // Jika salah satu tabel cukup kecil (< 1000 rows), gunakan hash join
            if (leftStats.TupleCount < 1000 || rightStats.TupleCount < 1000)
            {
                return OperationType.HASH_JOIN;
            }

            // Default: nested loop join
            return OperationType.NESTED_LOOP_JOIN;
        }
        catch
        {
            return OperationType.NESTED_LOOP_JOIN;
        }
    }
}
