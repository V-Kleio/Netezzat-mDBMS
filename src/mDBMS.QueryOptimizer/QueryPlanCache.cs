using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer;

/// <summary>
/// Cache in memory untuk optimasi rencana eksekusi kueri.
/// Menggunakan TTL-based expiration (kedaluwarsa) dan algoritma basic LRU (Least Recently Used) eviction.
/// </summary>
internal sealed class QueryPlanCache
{
    private sealed class CacheEntry
    {
        public CacheEntry(QueryPlan plan)
        {
            Plan = plan;
            CreatedAtUtc = DateTime.UtcNow;
            LastAccessedAtUtc = CreatedAtUtc;
        }
        public QueryPlan Plan { get; }
        public DateTime CreatedAtUtc { get; }
        public DateTime LastAccessedAtUtc { get; set; }
    }
    private readonly Dictionary<string, CacheEntry> entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly object syncRoot = new();
    private readonly TimeSpan timeToLive;
    private readonly int capacity;

    public QueryPlanCache(int capacity, TimeSpan timeToLive)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        if (timeToLive <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeToLive));

        this.capacity = capacity;
        this.timeToLive = timeToLive;
    }

    /// <summary>
    /// Mencoba mendapatkan rencana eksekusi dari cache berdasarkan key yang diberikan.
    /// Plan akan di-clone sebelum dikembalikan.
    /// </summary>
    public bool TryGet(string key, out QueryPlan plan)
    {
        lock (syncRoot)
        {
            if (entries.TryGetValue(key, out var entry))
            {
                if (IsExpired(entry))
                {
                    entries.Remove(key);
                }
                else
                {
                    entry.LastAccessedAtUtc = DateTime.UtcNow;
                    plan = QueryPlanCloner.Clone(entry.Plan);
                    return true;
                }
            }
        }

        plan = null!;
        return false;
    }

    public void Set(string key, QueryPlan plan)
    {
        var entry = new CacheEntry(QueryPlanCloner.Clone(plan));

        lock (syncRoot)
        {
            if (entries.Count >= capacity)
            {
                EvictLeastRecentlyUsed();
            }

            entries[key] = entry;
        }
    }

    public void Clear()
    {
        lock (syncRoot)
        {
            entries.Clear();
        }
    }

    private bool IsExpired(CacheEntry entry)
    {
        return (DateTime.UtcNow - entry.CreatedAtUtc) > timeToLive;
    }

    private void EvictLeastRecentlyUsed()
    {
        if (entries.Count == 0) return;

        var victim = entries.Aggregate((curr, next) =>
            curr.Value.LastAccessedAtUtc <= next.Value.LastAccessedAtUtc ? curr : next);

        entries.Remove(victim.Key);
    }
}