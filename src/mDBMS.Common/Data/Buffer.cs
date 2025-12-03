using System;
using System.Collections.Generic;

namespace mDBMS.Common.Data
{
    // Buffer Key
    public readonly struct BufferKey : IEquatable<BufferKey>
    {
        public string TableName { get; }
        public int BlockID { get; }

        public BufferKey(string tableName, int blockId)
        {
            TableName = tableName;
            BlockID = blockId;
        }

        public bool Equals(BufferKey other)
        {
            return TableName == other.TableName && BlockID == other.BlockID;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(TableName, BlockID);
        }
    }

    // Buffer pool
    public class BufferPool
    {
        private const int MaxBufferSize = 100;

        private readonly Dictionary<BufferKey, Page> _frames;
        private readonly List<BufferKey> _evictionQueue;

        public BufferPool()
        {
            _frames = new Dictionary<BufferKey, Page>();
            _evictionQueue = new List<BufferKey>();
        }

        public Page? GetPage(string tableName, int blockId)
        {
            var key = new BufferKey(tableName, blockId);

            if (_frames.TryGetValue(key, out var page))
            {
                // LRU Logic (posisi lama ke pling blakang)
                _evictionQueue.Remove(key);
                _evictionQueue.Add(key);
                return page;
            }
            return null;
        }

        public Page? AddOrUpdatePage(Page page)
        {
            var key = new BufferKey(page.TableName, page.BlockID);

            if (_frames.ContainsKey(key))
            {
                _frames[key] = page;
                
                // Update posisi LRU
                _evictionQueue.Remove(key);
                _evictionQueue.Add(key);
                
                return null;
            }

            Page? evictedPage = null;

            if (_frames.Count >= MaxBufferSize)
            {
                var victimKey = _evictionQueue[0];
                evictedPage = _frames[victimKey];

                _frames.Remove(victimKey);
                _evictionQueue.RemoveAt(0);
            }

            _frames.Add(key, page);
            _evictionQueue.Add(key);

            return evictedPage;
        }

        public IEnumerable<Page> GetAllPages()
        {
            return _frames.Values;
        }

        public List<Page> GetDirtyPages()
        {
            return _frames.Values.Where(p => p.IsDirty).ToList();
        }


        public void MarkClean(string tableName, int blockId)
        {
            var key = new BufferKey(tableName, blockId);
            if (_frames.TryGetValue(key, out var page))
            {
                page.IsDirty = false;
            }
        }

        public List<Page> FlushDirties()
        {
            var dirtyPages = GetDirtyPages();
            _frames.Clear();
            _evictionQueue.Clear();
            return dirtyPages;
        }
    }
}