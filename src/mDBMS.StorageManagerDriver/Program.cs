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
        Console.WriteLine("=== FINAL TESTING STORAGE MANAGER (WITH FREE SPACE MGMT) ===\n");

        // 1. Setup Storage Engine
        IStorageManager storage = new StorageEngine();
        string pathRoot = AppDomain.CurrentDomain.BaseDirectory;

        // --- PHASE 0: CLEAN UP & SEED ---
        CleanUp(pathRoot, new[] { "students.dat", "courses.dat", "enrollments.dat" });

        Console.WriteLine(">>> Melakukan Seeding Data Ulang (50 rows per table)...");
        Seeder.RunSeeder();
        Console.WriteLine(">>> Seeding Selesai.\n");

        // ==========================================
        // TEST A: READ & VERIFIKASI SEEDING
        // ==========================================
        Console.WriteLine("--- TEST A: Membaca Data Seeder ---");
        var requestRead = new DataRetrieval("Students", new[] { "*" }, null);
        int count = 0;
        foreach (var row in storage.ReadBlock(requestRead)) count++;
        
        if (count == 50) Console.WriteLine("SUCCESS: Read Seeder (50 rows) berfungsi!\n");
        else Console.WriteLine($"ERROR: Terbaca {count} baris.\n");

        // ==========================================
        // TEST B: STATISTIK
        // ==========================================
        Console.WriteLine("--- TEST B: Statistik ---");
        var stats = storage.GetStats("Students");
        Console.WriteLine($"[STATS] n_r: {stats.TupleCount}, b_r: {stats.BlockCount}");

        // ==========================================
        // TEST C & I: FREE SPACE MANAGEMENT (LOGIKA WRITE First-Fit)
        // ==========================================
        Console.WriteLine("--- TEST I: Free Space Management (First-Fit Strategy) ---");
        
        string studentsPath = Path.Combine(pathRoot, "students.dat");
        long sizeInitial = new FileInfo(studentsPath).Length;
        
        Console.WriteLine($"Ukuran File Awal: {sizeInitial} bytes (Header + 1 Data Block)");

        // Logika Lama (Append): Pasti nambah 4096 bytes (jadi block baru).
        // Logika Baru (First-Fit): Harusnya masuk ke block yang sudah ada (karena block 1 belum penuh).
        
        Console.WriteLine(">>> Menyisipkan Student ID 100 (Harusnya masuk ke celah kosong)...");
        var newValues = new Dictionary<string, object>
        {
            { "StudentID", 100 },
            { "FullName", "Mahasiswa Hemat Ruang" }
        };
        storage.WriteBlock(new DataWrite("Students", newValues, null));

        long sizeAfterInsert = new FileInfo(studentsPath).Length;
        Console.WriteLine($"Ukuran File Setelah Insert: {sizeAfterInsert} bytes");

        if (sizeAfterInsert == sizeInitial)
        {
            Console.WriteLine("SUCCESS: Ukuran file TIDAK bertambah!"); 
            Console.WriteLine("         (Data berhasil disisipkan ke blok yang ada / First-Fit working)\n");
        }
        else if (sizeAfterInsert > sizeInitial)
        {
            Console.WriteLine("WARNING: Ukuran file BERTAMBAH.");
            Console.WriteLine("         (Logika masih Append-Only atau Blok benar-benar penuh)\n");
        }

        // Verifikasi Data Masuk
        var checkReq = new DataRetrieval("Students", new[] { "*" }, new Condition { lhs="StudentID", opr=Condition.Operation.EQ, rhs="100" });
        bool found = false;
        foreach(var r in storage.ReadBlock(checkReq)) found = true;
        
        if(found) Console.WriteLine("SUCCESS: Data Student 100 terbaca kembali.\n");
        else Console.WriteLine("ERROR: Data Student 100 HILANG (Gagal tulis).\n");


        // ==========================================
        // TEST F: DELETE
        // ==========================================
        Console.WriteLine("--- TEST F: Menghapus Data (DeleteBlock) ---");
        
        // Hapus ID 100 yang baru kita buat
        var delReq = new DataDeletion("Students", new Condition { lhs="StudentID", opr=Condition.Operation.EQ, rhs="100" });
        int deleted = storage.DeleteBlock(delReq);
        Console.WriteLine($"Deleted Rows: {deleted}");

        // Cek lagi apakah ID 100 hilang
        found = false;
        foreach(var r in storage.ReadBlock(checkReq)) found = true;

        if(!found) Console.WriteLine("SUCCESS: Data berhasil dihapus.\n");
        else Console.WriteLine("ERROR: Data masih ada.\n");

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