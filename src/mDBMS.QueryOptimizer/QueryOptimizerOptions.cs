namespace mDBMS.QueryOptimizer;

/// <summary>
/// Options untuk <see cref="QueryOptimizerEngine"/>.
/// </summary>

public sealed class QueryOptimizerOptions {

    public static QueryOptimizerOptions Default { get; } = new QueryOptimizerOptions();

    /// <summary>
    /// Mengaktifkan caching rencana eksekusi yang dihasilkan oleh Query Optimizer.
    /// </summary>
    public bool EnablePlanCaching { get; init; } = true;

    /// <summary>
    /// Kapasitas entries pada cache rencana eksekusi.
    /// </summary>
    public int PlanCacheCapacity { get; init; } = 128;

    /// <summary>
    /// Waktu hidup (time-to-live) untuk setiap entry pada cache rencana eksekusi.
    /// </summary>
    public TimeSpan PlanCacheTTL { get; init; } = TimeSpan.FromMinutes(10);
}