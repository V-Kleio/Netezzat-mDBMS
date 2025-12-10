using mDBMS.Common.Data;

namespace mDBMS.StorageManager
{
    public static class StandaloneDemoSeeder
    {
        private static readonly Random _rand = new(123);
        private static readonly string[] DemoFiles = { "customers.dat", "products.dat", "orders.dat" };

        public static void Run(string dataPath = "")
        {
            if (string.IsNullOrWhiteSpace(dataPath))
            {
                dataPath = AppDomain.CurrentDomain.BaseDirectory;
            }

            Console.WriteLine("==============================================");
            Console.WriteLine("Standalone Demo Seeder (Customers/Products/Orders)");
            Console.WriteLine("==============================================");
            Console.WriteLine($"Target Directory: {dataPath}");
            Console.WriteLine();

            if (!Directory.Exists(dataPath))
            {
                Directory.CreateDirectory(dataPath);
            }

            CleanupExistingFiles(dataPath);
            SeedCustomers(dataPath);
            var priceMap = SeedProducts(dataPath);
            SeedOrders(dataPath, priceMap);

            Console.WriteLine();
            Console.WriteLine("Demo seed complete.");
            Console.WriteLine("==============================================");
        }

        private static void CleanupExistingFiles(string dataPath)
        {
            foreach (var file in DemoFiles)
            {
                string path = Path.Combine(dataPath, file);
                if (File.Exists(path))
                {
                    File.Delete(path);
                    Console.WriteLine($"Deleted existing: {file}");
                }
            }
            Console.WriteLine();
        }

        private static void SeedCustomers(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Customers",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "CustomerID", Type = DataType.Int, Length = 4 },
                    new() { Name = "FullName", Type = DataType.String, Length = 50 },
                    new() { Name = "City", Type = DataType.String, Length = 30 },
                    new() { Name = "Tier", Type = DataType.String, Length = 10 }
                }
            };

            string filePath = Path.Combine(dataPath, "customers.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            var entries = new (int Id, string Name, string City, string Tier)[]
            {
                (1, "Alice Johnson", "Jakarta", "Gold"),
                (2, "Budi Santoso", "Bandung", "Silver"),
                (3, "Citra Dewi", "Surabaya", "Gold"),
                (4, "Dimas Pratama", "Yogyakarta", "Bronze"),
                (5, "Eka Putri", "Jakarta", "Silver"),
                (6, "Fajar Hadi", "Semarang", "Gold"),
                (7, "Gita Lestari", "Denpasar", "Silver"),
                (8, "Hendra Gunawan", "Medan", "Bronze"),
            };

            List<byte[]> rows = new();
            foreach (var c in entries)
            {
                var row = new Row { id = $"CUS-{c.Id:D3}" };
                row.Columns["CustomerID"] = c.Id;
                row.Columns["FullName"] = c.Name;
                row.Columns["City"] = c.City;
                row.Columns["Tier"] = c.Tier;
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Customers: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }

        private static Dictionary<int, float> SeedProducts(string dataPath)
        {
            var schema = new TableSchema
            {
                TableName = "Products",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "ProductID", Type = DataType.Int, Length = 4 },
                    new() { Name = "ProductName", Type = DataType.String, Length = 50 },
                    new() { Name = "Category", Type = DataType.String, Length = 20 },
                    new() { Name = "Price", Type = DataType.Float, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "products.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            var entries = new (int Id, string Name, string Category, float Price)[]
            {
                (1, "Laptop 14\"", "Electronics", 14500000f),
                (2, "Wireless Mouse", "Accessories", 250000f),
                (3, "Mechanical Keyboard", "Accessories", 950000f),
                (4, "Monitor 24\"", "Electronics", 2550000f),
                (5, "Noise Cancelling Headset", "Accessories", 850000f),
                (6, "Backpack", "Lifestyle", 450000f)
            };

            List<byte[]> rows = new();
            var priceMap = new Dictionary<int, float>();

            foreach (var p in entries)
            {
                var row = new Row { id = $"PRD-{p.Id:D3}" };
                row.Columns["ProductID"] = p.Id;
                row.Columns["ProductName"] = p.Name;
                row.Columns["Category"] = p.Category;
                row.Columns["Price"] = p.Price;
                priceMap[p.Id] = p.Price;
                rows.Add(RowSerializer.SerializeRow(schema, row));
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Products: {rows.Count} rows -> {Path.GetFileName(filePath)}");
            return priceMap;
        }

        private static void SeedOrders(string dataPath, Dictionary<int, float> priceLookup)
        {
            var schema = new TableSchema
            {
                TableName = "Orders",
                Columns = new List<ColumnSchema>
                {
                    new() { Name = "OrderID", Type = DataType.Int, Length = 4 },
                    new() { Name = "CustomerID", Type = DataType.Int, Length = 4 },
                    new() { Name = "ProductID", Type = DataType.Int, Length = 4 },
                    new() { Name = "Quantity", Type = DataType.Int, Length = 4 },
                    new() { Name = "Status", Type = DataType.String, Length = 12 },
                    new() { Name = "OrderTotal", Type = DataType.Float, Length = 4 }
                }
            };

            string filePath = Path.Combine(dataPath, "orders.dat");
            SchemaSerializer.WriteSchema(filePath, schema);

            string[] statuses = { "PAID", "SHIPPED", "PROCESS", "CANCELLED" };
            List<byte[]> rows = new();

            int orderId = 1;
            for (int i = 0; i < 20; i++)
            {
                int custId = _rand.Next(1, 9);      // 1..8
                int prodId = _rand.Next(1, 7);      // 1..6
                int qty = _rand.Next(1, 5);         // 1..4
                string status = statuses[_rand.Next(statuses.Length)];

                float price = priceLookup[prodId];
                float total = price * qty;

                var row = new Row { id = $"ORD-{orderId:D4}" };
                row.Columns["OrderID"] = orderId;
                row.Columns["CustomerID"] = custId;
                row.Columns["ProductID"] = prodId;
                row.Columns["Quantity"] = qty;
                row.Columns["Status"] = status;
                row.Columns["OrderTotal"] = total;
                rows.Add(RowSerializer.SerializeRow(schema, row));

                orderId++;
            }

            WriteRowsToBlocks(filePath, rows);
            Console.WriteLine($"Orders: {rows.Count} rows -> {Path.GetFileName(filePath)}");
        }

        private static void WriteRowsToBlocks(string filePath, List<byte[]> rows)
        {
            List<byte[]> currentBlock = new();
            int currentSize = 4; // record directory overhead

            foreach (var rowBytes in rows)
            {
                if (currentSize + rowBytes.Length + 2 > BlockSerializer.BlockSize)
                {
                    var block = BlockSerializer.CreateBlock(currentBlock);
                    BlockSerializer.AppendBlockToFile(filePath, block);
                    currentBlock.Clear();
                    currentSize = 4;
                }

                currentBlock.Add(rowBytes);
                currentSize += rowBytes.Length + 2;
            }

            if (currentBlock.Count > 0)
            {
                var block = BlockSerializer.CreateBlock(currentBlock);
                BlockSerializer.AppendBlockToFile(filePath, block);
            }
        }
    }
}
