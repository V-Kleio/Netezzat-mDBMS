using mDBMS.Common.Data;

namespace mDBMS.FailureRecovery
{
    /// <summary>
    /// Buffer Pool untuk menyimpan pages di memory sebelum di-flush ke disk
    /// Menggunakan LRU (Least Recently Used) eviction policy
    /// </summary>
    public class BufferPool
    {
        private readonly Dictionary<string, Page> _buffer; // Key: "tableName_blockId"
        private readonly LinkedList<string> _lruList; // LRU tracking
        private readonly int _maxSize;
        private readonly object _lock = new object();

        public BufferPool(int maxSize = 100)
        {
            _buffer = new Dictionary<string, Page>();
            _lruList = new LinkedList<string>();
            _maxSize = maxSize;
        }

        /// <summary>
        /// Ambil page dari buffer pool
        /// </summary>
        public Page? GetPage(string tableName, int blockId)
        {
            lock (_lock)
            {
                string key = GetKey(tableName, blockId);

                if (_buffer.TryGetValue(key, out Page? page))
                {
                    // Update LRU - move to end (most recently used)
                    _lruList.Remove(key);
                    _lruList.AddLast(key);

                    return page;
                }

                return null;
            }
        }

        /// <summary>
        /// Tambahkan atau update page di buffer pool
        /// Return: evicted page jika ada (bisa dirty)
        /// </summary>
        public Page? AddOrUpdatePage(Page page)
        {
            lock (_lock)
            {
                string key = GetKey(page.TableName, page.BlockID);
                Page? evictedPage = null;

                // Jika page sudah ada, update saja
                if (_buffer.ContainsKey(key))
                {
                    _buffer[key] = page;

                    // Update LRU
                    _lruList.Remove(key);
                    _lruList.AddLast(key);

                    return null;
                }

                // Jika buffer penuh, evict LRU page
                if (_buffer.Count >= _maxSize)
                {
                    evictedPage = EvictLRU();
                }

                // Tambahkan page baru
                _buffer[key] = page;
                _lruList.AddLast(key);

                return evictedPage;
            }
        }

        /// <summary>
        /// Ambil semua dirty pages (pages yang perlu di-flush)
        /// </summary>
        public List<Page> GetDirtyPages()
        {
            lock (_lock)
            {
                return _buffer.Values
                    .Where(p => p.IsDirty)
                    .ToList();
            }
        }

        /// <summary>
        /// Flush (kosongkan) semua pages dari buffer
        /// Return: list of dirty pages yang perlu ditulis ke disk
        /// </summary>
        public List<Page> FlushAll()
        {
            lock (_lock)
            {
                var dirtyPages = GetDirtyPages();

                // Clear buffer dan LRU list
                _buffer.Clear();
                _lruList.Clear();

                return dirtyPages;
            }
        }

        /// <summary>
        /// Mark page sebagai clean (setelah berhasil ditulis ke disk)
        /// </summary>
        public void MarkClean(string tableName, int blockId)
        {
            lock (_lock)
            {
                string key = GetKey(tableName, blockId);

                if (_buffer.TryGetValue(key, out Page? page))
                {
                    page.IsDirty = false;
                }
            }
        }

        /// <summary>
        /// Evict LRU (Least Recently Used) page
        /// </summary>
        private Page? EvictLRU()
        {
            if (_lruList.First == null)
                return null;

            string lruKey = _lruList.First.Value;
            _lruList.RemoveFirst();

            if (_buffer.TryGetValue(lruKey, out Page? evictedPage))
            {
                _buffer.Remove(lruKey);
                return evictedPage;
            }

            return null;
        }

        private string GetKey(string tableName, int blockId)
        {
            return $"{tableName}_{blockId}";
        }

        public int Count => _buffer.Count;
    }
}
