using mDBMS.Common.Interfaces;
using mDBMS.Common.QueryData;
using mDBMS.Common.Data;

namespace mDBMS.QueryProcessor.DML;

class ProjectionOperator : Operator
{
    public ProjectionOperator(IStorageManager storageManager, QueryPlanStep queryPlanStep, LocalTableStorage localTableStorage)
        : base(storageManager, queryPlanStep, localTableStorage)
    {
        // Operator ini bekerja dengan data hasil dari operator sebelumnya (di memori) bukan membaca langsung dari disk
        this.usePreviousTable = true;
    }

    public override IEnumerable<Row> GetRows()
    {
        // 1. Ambil data input (tongkat estafet dari operator sebelumnya)
        var inputRows = localTableStorage.lastResult;
        if (inputRows == null || !inputRows.Any())
        {
            yield break;
        }

        // 2. Tentukan kolom target (Parsing dipisah agar kode ini bersih)
        var targetColumns = ParseColumnsFromDescription(queryPlanStep.Description);
        bool selectAll = targetColumns.Contains("*");

        // 3. Proses Proyeksi (Looping & Salin Data)
        foreach (var row in inputRows)
        {
            var projectedRow = new Row();

            if (selectAll)
            {
                // Jika SELECT *, salin semua kolom
                foreach (var col in row.Columns)
                {
                    projectedRow.Columns[col.Key] = col.Value;
                }
            }
            else
            {
                // Jika kolom spesifik, salin hanya yang diminta
                foreach (var colName in targetColumns)
                {
                    // Cek apakah kolom ada di data asal
                    if (row.Columns.ContainsKey(colName))
                    {
                        projectedRow.Columns[colName] = row.Columns[colName];
                    }
                }
            }

            yield return projectedRow;
        }
    }

    /// <summary>
    /// Helper untuk mengambil nama kolom dari string deskripsi.
    /// Memisahkan logika "kotor" manipulasi string dari logika utama.
    /// </summary>
    private List<string> ParseColumnsFromDescription(string description)
    {
        // Format dari QueryOptimizer: "Project columns: col1, col2"
        string prefix = "Project columns: ";
        
        if (!string.IsNullOrEmpty(description) && description.StartsWith(prefix))
        {
            // Hapus prefix, lalu pecah berdasarkan koma
            var columnsPart = description.Replace(prefix, "").Trim();
            return columnsPart.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
        }

        // Default: Jika gagal parse atau kosong, anggap SELECT *
        return new List<string> { "*" };
    }
}