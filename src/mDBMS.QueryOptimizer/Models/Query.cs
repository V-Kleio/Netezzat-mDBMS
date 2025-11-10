namespace mDBMS.QueryOptimizer.Models
{
    /// <summary>
    /// Enum type operasi query
    /// </summary>
    public enum QueryType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }

    /// <summary>
    /// Tipe operasi JOIN
    /// </summary>
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL
    }

    /// <summary>
    /// Operasi JOIN
    /// </summary>
    public class JoinOperation {
        public string LeftTable { get; set; } = string.Empty;
        public string RightTable { get; set; } = string.Empty;
        public string OnCondition { get; set; } = string.Empty;
        public JoinType Type { get; set; } = JoinType.INNER;
    }

    /// <summary>
    /// Operasi ORDER BY
    /// </summary>
    public class OrderByOperation {
        public string Column { get; set; } = string.Empty;
        public bool IsAscending { get; set; } = true;
    }


    public class Query {
        /// <summary>
        /// Tabel query target
        /// </summary>
        public string Table { get; set; } = string.Empty;

        /// <summary>
        /// Kolom yang dipilih
        /// </summary>
        public List<string> SelectedColumns { get; set; } = new List<string>();

        /// <summary>
        /// Kondisi WHERE
        /// </summary>
        public string? WhereClause { get; set; }

        /// <summary>
        /// Operasi Join
        /// </summary>
        public List<JoinOperation>? Joins { get; set; }

        /// <summary>
        /// Operasi Order By
        /// </summary>
        public List<OrderByOperation>? OrderBy { get; set; }

        /// <summary>
        /// Operasi Group By
        /// </summary>
        public List<string>? GroupBy { get; set; } = new List<string>();

        /// <summary>
        /// Tipe operasi query (SELECT, INSERT, UPDATE, DELETE)
        /// </summary>
        public QueryType Type { get; set; } = QueryType.SELECT;
    }


}