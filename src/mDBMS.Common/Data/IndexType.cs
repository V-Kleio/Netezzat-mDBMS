namespace mDBMS.Common.Data;

/// <summary>
/// Specifies the type of index to use for database operations.
/// </summary>
public enum IndexType
{
    /// <summary>
    /// No index is used.
    /// </summary>
    None,
    /// <summary>
    /// B-Tree index, suitable for range queries and ordered data. Provides good performance for insert, delete, and search operations.
    /// </summary>
    BTree,
    /// <summary>
    /// Hash index, suitable for exact match queries. Provides fast lookup for equality comparisons but does not support range queries.
    /// </summary>
    Hash
}