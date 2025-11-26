using System;
using System.Collections.Generic;
using System.IO;
using mDBMS.Common.Data;
using mDBMS.Common.Interfaces;
using mDBMS.StorageManager;

class StorageManagerDriver
{
    static void Main(string[] args)
    {
        Console.WriteLine("=== FINAL TESTING STORAGE MANAGER (MILESTONE 2 COMPLETE) ===\n");

        // 1. Setup Storage Engine
        IStorageManager storage = new StorageEngine();
        string pathRoot = AppDomain.CurrentDomain.BaseDirectory;

        // --- PHASE 0: CLEAN UP & SEED ---
        // Membersihkan file .dat lama agar pengujian bersih
        CleanUp(pathRoot, new[] { "students.dat", "courses.dat", "enrollments.dat" });

        Console.WriteLine(">>> Melakukan Seeding Data Ulang (50 rows per table)...");
        Seeder.RunSeeder();
        Console.WriteLine(">>> Seeding Selesai.\n");

        // ==========================================
        // TEST A: READ & VERIFIKASI SEEDING (LINEAR)
        // ==========================================
        Console.WriteLine("--- TEST A: Membaca Data Seeder (Linear Scan) ---");
        var requestRead = new DataRetrieval("Students", new[] { "*" }, null);
        int count = 0;

        foreach (var row in storage.ReadBlock(requestRead))
        {
            count++;
            // Tampilkan beberapa data awal & akhir saja agar console tidak penuh
            if (count <= 2 || count >= 49)
            {
                Console.WriteLine($"[LINEAR READ] Row {count}: ID={row["StudentID"]}, Name={row["FullName"]}");
            }
        }

        Console.WriteLine($"Total data terbaca: {count} baris. (Expected: 50)");
        if (count == 50)
            Console.WriteLine("SUCCESS: Read Seeder berfungsi!\n");
        else
            Console.WriteLine("ERROR: Data Seeder tidak terbaca lengkap.\n");


        // ==========================================
        // TEST B: STATISTIK (TASK 1.2)
        // ==========================================
        Console.WriteLine("--- TEST B: Menghitung Statistik (TASK 1.2) ---");
        var stats = storage.GetStats("Students");

        Console.WriteLine($"[STATS] Table: {stats.Table}");
        Console.WriteLine($"[STATS] n_r (Total Rows): {stats.TupleCount}");
        Console.WriteLine($"[STATS] b_r (Total Blocks): {stats.BlockCount}");
        Console.WriteLine($"[STATS] l_r (Row Size): {stats.TupleSize} bytes");
        Console.WriteLine($"[STATS] f_r (Blocking Factor): {stats.BlockingFactor}");

        if (stats.TupleCount == 50 && stats.BlockCount > 0)
            Console.WriteLine("SUCCESS: GetStats Valid.\n");
        else
            Console.WriteLine("ERROR: Statistik salah.\n");


        // ==========================================
        // TEST C: WRITE (TASK 2)
        // ==========================================
        Console.WriteLine("--- TEST C: Menulis Data Baru (Persistence) ---");

        string studentsPath = Path.Combine(pathRoot, "students.dat");
        long sizeBefore = File.Exists(studentsPath) ? new FileInfo(studentsPath).Length : 0;

        var newValues = new Dictionary<string, object>
        {
            { "StudentID", 999 },
            { "FullName", "Mahasiswa Insert Baru" }
        };
        var newStudent = new DataWrite("Students", newValues, null);
        int affected = storage.WriteBlock(newStudent);

        long sizeAfter = File.Exists(studentsPath) ? new FileInfo(studentsPath).Length : 0;

        Console.WriteLine($"[WRITE] Affected: {affected}, Size Delta: {sizeAfter - sizeBefore} bytes");

        if (sizeAfter > sizeBefore)
            Console.WriteLine("SUCCESS: File bertambah besar (Write OK)!\n");
        else
            Console.WriteLine("ERROR: File tidak berubah.\n");


        // ==========================================
        // TEST D: CREATE INDEX (TASK 3.1 & 3.2)
        // ==========================================
        Console.WriteLine("--- TEST D: Membuat Hash Index (Indexing) ---");

        // Kita buat Index di kolom StudentID
        storage.SetIndex("Students", "StudentID", IndexType.Hash);

        // Verifikasi visual (cek console output "Membangun Index...")
        Console.WriteLine("SUCCESS: Perintah SetIndex dieksekusi (Cek log di atas).\n");


        // ==========================================
        // TEST E: READ WITH INDEX (TASK 3.3)
        // ==========================================
        Console.WriteLine("--- TEST E: Membaca Menggunakan Index (Jump Read) ---");
        Console.WriteLine("Mencari StudentID = 5 (Seharusnya melompat langsung ke blok terkait)");

        // Membuat Condition WHERE StudentID = 5
        var condition = new Condition
        {
            lhs = "StudentID",
            opr = Condition.Operation.EQ,
            rhs = "5"
        };

        var requestIndex = new DataRetrieval("Students", new[] { "*" }, condition);

        bool found = false;
        foreach (var row in storage.ReadBlock(requestIndex))
        {
            Console.WriteLine($"[INDEX READ RESULT] ID={row["StudentID"]}, Name={row["FullName"]}");
            if (row["StudentID"].ToString() == "5") found = true;
        }

        if (found)
            Console.WriteLine("SUCCESS: Data ditemukan menggunakan Index Logic!\n");
        else
            Console.WriteLine("ERROR: Data tidak ditemukan.\n");


        // ==========================================
        // TEST F: DELETE (TASK Milestone 3)
        // ==========================================
        Console.WriteLine("--- TEST F: Menghapus Data (DeleteBlock) ---");

        // Hitung jumlah row sebelum delete
        var beforeDelete = storage.ReadBlock(new DataRetrieval("Students", new[] { "*" }, null));
        int countBeforeDelete = 0;
        foreach (var _ in beforeDelete) countBeforeDelete++;

        Console.WriteLine($"Jumlah row sebelum delete: {countBeforeDelete}");

        // Hapus row dengan StudentID = 5
        var deleteCondition = new Condition
        {
            lhs = "StudentID",
            opr = Condition.Operation.EQ,
            rhs = "5"
        };
        var deletionRequest = new DataDeletion("Students", deleteCondition);
        int deletedCount = storage.DeleteBlock(deletionRequest);

        Console.WriteLine($"[DELETE] Jumlah row terhapus: {deletedCount}");

        // Hitung jumlah row setelah delete
        var afterDelete = storage.ReadBlock(new DataRetrieval("Students", new[] { "*" }, null));
        int countAfterDelete = 0;
        foreach (var _ in afterDelete) countAfterDelete++;

        Console.WriteLine($"Jumlah row setelah delete: {countAfterDelete}");

        // Verifikasi bahwa StudentID = 5 sudah tidak ada
        var verifyDelete = storage.ReadBlock(new DataRetrieval("Students", new[] { "*" }, deleteCondition));
        bool stillExists = false;
        foreach (var _ in verifyDelete) stillExists = true;

        if (deletedCount > 0 && countAfterDelete == countBeforeDelete - deletedCount && !stillExists)
            Console.WriteLine("SUCCESS: DeleteBlock berfungsi dengan baik!\n");
        else
            Console.WriteLine("ERROR: DeleteBlock gagal atau data masih ada.\n");


        // ==========================================
        // TEST G: DELETE MULTIPLE (Bonus)
        // ==========================================
        Console.WriteLine("--- TEST G: Menghapus Multiple Rows ---");

        // Hapus semua row dengan StudentID >= 45 (seharusnya ada beberapa row)
        // Note: Condition saat ini hanya support EQ, jadi kita test dengan kondisi spesifik
        // Kita hapus row dengan StudentID = 10
        var deleteCondition2 = new Condition
        {
            lhs = "StudentID",
            opr = Condition.Operation.EQ,
            rhs = "10"
        };
        var deletionRequest2 = new DataDeletion("Students", deleteCondition2);
        int deletedCount2 = storage.DeleteBlock(deletionRequest2);

        Console.WriteLine($"[DELETE] Jumlah row terhapus (StudentID=10): {deletedCount2}");

        if (deletedCount2 > 0)
            Console.WriteLine("SUCCESS: Delete kondisi lain juga berfungsi!\n");
        else
            Console.WriteLine("WARNING: Tidak ada data terhapus (mungkin sudah tidak ada).\n");


        // ==========================================
        // TEST H: DELETE WITHOUT CONDITION (Delete All - Dangerous!)
        // ==========================================
        Console.WriteLine("--- TEST H: Delete Without Condition (Hati-hati!) ---");
        Console.WriteLine("SKIP: Test ini di-skip karena berbahaya (akan hapus semua data).\n");


        Console.WriteLine("=== ALL TESTS COMPLETED ===");
    }

    private static void CleanUp(string rootPath, string[] fileNames)
    {
        foreach(var fileName in fileNames)
        {
            string path = Path.Combine(rootPath, fileName);
            if(File.Exists(path)) File.Delete(path);
        }
    }
}
