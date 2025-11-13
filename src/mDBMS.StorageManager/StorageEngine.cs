
using mDBMS.Common.Interfaces;
using mDBMS.Common.Models;

namespace mDBMS.StorageManager
{
    public class StorageEngine : IStorageManager
    {
        // konstruktor untuk menerima dependensi jika diperlukan nanti
        // misal IBufferManager dari Failure Recovery Manager
        public StorageEngine()
        {
            // untuk fase 1, konstruktor masih kosong
        }

        public IEnumerable<Row> ReadBlock(DataRetrieval dataRetrieval)
        {
            // stub untuk fase 1 - hanya mencetak pesan dan return dummy data
            Console.WriteLine($"[STUB SM]: ReadBlock dipanggil untuk tabel '{dataRetrieval.Table}'");
            Console.WriteLine($"[STUB SM]: Kolom yang diminta: {string.Join(", ", dataRetrieval.Columns)}");
            Console.WriteLine($"[STUB SM]: Kondisi: {dataRetrieval.Condition ?? "tanpa kondisi"}");

            // return data dummy yang hardcoded
            var dummyRows = new List<Row>();
            
            // dummy data untuk Students
            if (dataRetrieval.Table == "Students")
            {
                for (int i = 1; i <= 5; i++)
                {
                    var row = new Row();
                    row["StudentID"] = i;
                    row["FullName"] = $"Student {i}";
                    dummyRows.Add(row);
                }
            }
            // dummy data untuk Courses
            else if (dataRetrieval.Table == "Courses")
            {
                for (int i = 1; i <= 5; i++)
                {
                    var row = new Row();
                    row["CourseID"] = i;
                    row["CourseName"] = $"Course {i}";
                    row["Credits"] = 3;
                    dummyRows.Add(row);
                }
            }
            // dummy data untuk Enrollments
            else if (dataRetrieval.Table == "Enrollments")
            {
                for (int i = 1; i <= 5; i++)
                {
                    var row = new Row();
                    row["EnrollmentID"] = i;
                    row["StudentID"] = i;
                    row["CourseID"] = i;
                    row["Grade"] = "A";
                    dummyRows.Add(row);
                }
            }

            Console.WriteLine($"[STUB SM]: Mengembalikan {dummyRows.Count} baris dummy");
            return dummyRows;
        }

        public int WriteBlock(DataWrite dataWrite)
        {
            // stub untuk fase 1 - hanya mencetak pesan dan return dummy value
            Console.WriteLine($"[STUB SM]: WriteBlock dipanggil untuk tabel '{dataWrite.Table}'");
            Console.WriteLine($"[STUB SM]: Data yang akan ditulis: {string.Join(", ", dataWrite.NewValues.Keys)}");
            Console.WriteLine($"[STUB SM]: Kondisi: {dataWrite.Condition ?? "tanpa kondisi (insert)"}");

            // return jumlah baris yang terpengaruh (dummy)
            int affectedRows = 1;
            Console.WriteLine($"[STUB SM]: {affectedRows} baris terpengaruh (dummy)");
            return affectedRows;
        }

        public int DeleteBlock(DataDeletion dataDeletion)
        {
            // stub untuk fase 1 - hanya mencetak pesan dan return dummy value
            Console.WriteLine($"[STUB SM]: DeleteBlock dipanggil untuk tabel '{dataDeletion.Table}'");
            Console.WriteLine($"[STUB SM]: Kondisi: {dataDeletion.Condition ?? "tanpa kondisi"}");

            // return jumlah baris yang dihapus (dummy)
            int deletedRows = 1;
            Console.WriteLine($"[STUB SM]: {deletedRows} baris dihapus (dummy)");
            return deletedRows;
        }

        public void SetIndex(string table, string column, IndexType type)
        {
            // stub untuk fase 1 - hanya mencetak pesan
            Console.WriteLine($"[STUB SM]: SetIndex dipanggil");
            Console.WriteLine($"[STUB SM]: Tabel: {table}, Kolom: {column}, Tipe Index: {type}");
            Console.WriteLine($"[STUB SM]: Index berhasil dibuat (dummy)");
        }

        public Statistic GetStats()
        {
            // stub untuk fase 1 - return statistik hardcoded sesuai data di file .dat
            Console.WriteLine($"[STUB SM]: GetStats dipanggil");

            // statistik dummy untuk ketiga tabel
            var stats = new Statistic
            {
                Table = "Students",
                TupleCount = 50,        // jumlah baris yang ada di students.dat
                BlockCount = 1,          // estimasi jumlah blok (50 rows * ~54 bytes/row / 4096 bytes)
                TupleSize = 54,          // 4 bytes (int) + 50 bytes (varchar) = 54 bytes per row
                BlockingFactor = 75,     // berapa banyak tuple per blok (4096 / 54 ≈ 75)
                DistinctValues = 50      // semua StudentID unique
            };

            Console.WriteLine($"[STUB SM]: Statistik untuk tabel '{stats.Table}':");
            Console.WriteLine($"[STUB SM]:   - TupleCount: {stats.TupleCount}");
            Console.WriteLine($"[STUB SM]:   - BlockCount: {stats.BlockCount}");
            Console.WriteLine($"[STUB SM]:   - TupleSize: {stats.TupleSize} bytes");
            Console.WriteLine($"[STUB SM]:   - BlockingFactor: {stats.BlockingFactor}");
            Console.WriteLine($"[STUB SM]:   - DistinctValues: {stats.DistinctValues}");

            return stats;
        }
    }
}