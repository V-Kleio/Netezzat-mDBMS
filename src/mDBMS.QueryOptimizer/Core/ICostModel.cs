using mDBMS.Common.Data;

namespace mDBMS.QueryOptimizer.Core;

/// <summary>
/// Interface untuk model perhitungan cost execution plan.
/// Memisahkan logic costing dari plan structure, memungkinkan berbagai strategi:
/// - Cost-based: gunakan statistik nyata
/// - Rule-based: gunakan heuristic sederhana
/// - Adaptive: adjust berdasarkan runtime statistics
/// 
/// Principle: Single Responsibility - hanya fokus pada cost calculation
/// </summary>
public interface ICostModel
{
    /// <summary>
    /// Estimasi cost untuk table scan (full sequential scan).
    /// </summary>
    /// <param name="stats">Statistik tabel dari storage manager</param>
    /// <returns>Estimated cost</returns>
    double EstimateTableScan(Statistic stats);

    /// <summary>
    /// Estimasi cost untuk index scan (sequential scan pada index).
    /// </summary>
    /// <param name="stats">Statistik tabel</param>
    /// <returns>Estimated cost</returns>
    double EstimateIndexScan(Statistic stats);

    /// <summary>
    /// Estimasi cost untuk index seek (random access menggunakan index).
    /// </summary>
    /// <param name="stats">Statistik tabel</param>
    /// <param name="selectivity">Persentase baris yang akan dipilih (0.0 - 1.0)</param>
    /// <returns>Estimated cost</returns>
    double EstimateIndexSeek(Statistic stats, double selectivity);

    /// <summary>
    /// Estimasi cost untuk operasi filter (evaluasi kondisi WHERE).
    /// </summary>
    /// <param name="inputRows">Jumlah baris input</param>
    /// <param name="condition">Kondisi filter (untuk analisis complexity)</param>
    /// <returns>Estimated cost</returns>
    double EstimateFilter(double inputRows, string condition);

    /// <summary>
    /// Estimasi cost untuk operasi projection (pemilihan kolom).
    /// </summary>
    /// <param name="inputRows">Jumlah baris input</param>
    /// <param name="columnCount">Jumlah kolom yang dipilih</param>
    /// <returns>Estimated cost</returns>
    double EstimateProject(double inputRows, int columnCount);

    /// <summary>
    /// Estimasi cost untuk operasi sort.
    /// </summary>
    /// <param name="inputRows">Jumlah baris yang akan di-sort</param>
    /// <returns>Estimated cost (biasanya O(n log n))</returns>
    double EstimateSort(double inputRows);

    /// <summary>
    /// Estimasi cost untuk operasi aggregate/grouping.
    /// </summary>
    /// <param name="inputRows">Jumlah baris input</param>
    /// <param name="groupByCount">Jumlah kolom GROUP BY</param>
    /// <returns>Estimated cost</returns>
    double EstimateAggregate(double inputRows, int groupByCount);

    /// <summary>
    /// Estimasi cost untuk nested loop join.
    /// </summary>
    /// <param name="leftRows">Jumlah baris dari left input</param>
    /// <param name="rightRows">Jumlah baris dari right input</param>
    /// <returns>Estimated cost</returns>
    double EstimateNestedLoopJoin(double leftRows, double rightRows);

    /// <summary>
    /// Estimasi cost untuk hash join.
    /// </summary>
    /// <param name="leftRows">Jumlah baris dari left input</param>
    /// <param name="rightRows">Jumlah baris dari right input</param>
    /// <returns>Estimated cost</returns>
    double EstimateHashJoin(double leftRows, double rightRows);

    /// <summary>
    /// Estimasi cost untuk merge join.
    /// </summary>
    /// <param name="leftRows">Jumlah baris dari left input</param>
    /// <param name="rightRows">Jumlah baris dari right input</param>
    /// <returns>Estimated cost</returns>
    double EstimateMergeJoin(double leftRows, double rightRows);

    /// <summary>
    /// Estimasi selectivity (rasio output/input rows) dari suatu kondisi.
    /// Digunakan untuk menghitung estimated rows setelah filter.
    /// </summary>
    /// <param name="condition">Kondisi filter</param>
    /// <param name="stats">Statistik tabel untuk analisis</param>
    /// <returns>Selectivity factor (0.0 - 1.0)</returns>
    double EstimateSelectivity(string condition, Statistic stats);

    /// <summary>
    /// Estimasi cost untuk operasi UPDATE.
    /// </summary>
    /// <param name="affectedRows">Jumlah baris yang akan diupdate</param>
    /// <param name="blockSize">Ukuran blok penyimpanan (untuk menghitung I/O)</param>
    /// <returns>Estimated cost</returns>
    double EstimateUpdate(double affectedRows, double blockSize, int indexCount = 0);

    /// <summary>
    /// Estimasi cost untuk opperasi INSERT
    /// </summary>
    /// <param name="rowCount">Jumlah baris yang diinsert</param>
    /// <param name="columnCount">Jumlah kolom per baris (memengaruhi blok penyimpanan)</param>
    /// <param name="indexCount">Jumlah index yang perlu diupdate</param>
    /// <param name="hasConstraints">Foreign key, unique, check constraints</param>
    /// <returns>Estimated cost</returns>
    double EstimateInsert(double rowCount, int columnCount, int indexCount = 0, bool hasConstraints = false);

    /// <summary>
    /// Estimasi cost untuk operasi DELETE.
    /// </summary>
    /// <param name="affectedRows">Jumlah baris yang akan didelete</param>
    /// <param name="blockingFactor">Jumlah baris per blok penyimpanan</param>
    /// <param name="indexCount">Jumlah index yang perlu dimaintain</param>
    /// <param name="hasCascade">Apakah ada foreign key cascade delete</param>
    /// <returns>Estimated cost</returns>
    double EstimatedDelete(double affectedRows, double blockingFactor, int indexCount = 0, bool hasCascade = false);
}
