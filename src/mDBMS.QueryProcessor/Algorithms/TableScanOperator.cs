using System;
using System.Collections.Generic;
using System.Linq;
using mDBMS.Common.Interfaces;
using mDBMS.Common.Data;

namespace mDBMS.QueryProcessor.Algorithms
{
    public class BasicTableScanOperator
    {
        private readonly IStorageManager _storageManager;

        public BasicTableScanOperator(IStorageManager storageManager)
        {
            _storageManager = storageManager ?? throw new ArgumentNullException(nameof(storageManager));
        }

        public IEnumerable<Row> Execute(string tableName, IEnumerable<string>? columns = null, Condition? condition = null)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("Nama tabel wajib diisi.", nameof(tableName));
            }

            var normalizedColumns = NormalizeColumns(columns);
            var retrieval = new DataRetrieval(tableName, normalizedColumns, condition);
            var rawRows = _storageManager.ReadBlock(retrieval) ?? Enumerable.Empty<Row>();

            return rawRows.Select(row => PrefixColumns(row, tableName)).ToList();
        }

        private static string[] NormalizeColumns(IEnumerable<string>? columns)
        {
            if (columns is null)
            {
                return new[] { "*" };
            }

            var filtered = columns.Where(column => !string.IsNullOrWhiteSpace(column)).ToArray();
            return filtered.Length > 0 ? filtered : new[] { "*" };
        }

        private static Row PrefixColumns(Row sourceRow, string tableName)
        {
            var prefixedRow = new Row();
            foreach (var kvp in sourceRow.Columns)
            {
                var prefixedName = kvp.Key.Contains('.') ? kvp.Key : $"{tableName}.{kvp.Key}";
                prefixedRow.Columns[prefixedName] = kvp.Value;
            }

            return prefixedRow;
        }
    }
}
