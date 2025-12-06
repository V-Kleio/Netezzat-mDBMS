using Xunit;
using mDBMS.Common.Data;
using mDBMS.StorageManager;
using System.Collections.Generic;
using System;

namespace mDBMS.StorageManager.Tests
{
    public class RowSerializerTests
    {
        [Fact]
        public void SerializeRow_And_DeserializeRow_ShouldRetainDataIntegrity()
        {
            var columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "ID", Type = DataType.Int, Length = 4 },
                new ColumnSchema { Name = "Name", Type = DataType.String, Length = 20 },
                new ColumnSchema { Name = "GPA", Type = DataType.Float, Length = 4 }
            };
            var schema = new TableSchema { Columns = columns };

            var row = new Row();
            row.id = Guid.NewGuid().ToString();
            row["ID"] = 101;
            row["Name"] = "Test Student";
            row["GPA"] = 3.9f;

            byte[] serializedBytes = RowSerializer.SerializeRow(schema, row);

            Row resultRow = RowSerializer.DeserializeRow(schema, serializedBytes);
            Assert.NotNull(resultRow);
            Assert.Equal(row.id, resultRow.id);
            Assert.Equal(101, resultRow["ID"]);

            Assert.Equal("Test Student", resultRow["Name"]);
            Assert.Equal(3.9f, (float)resultRow["GPA"], 2);
        }

        [Fact]
        public void SerializeRow_ShouldTruncateString_WhenTooLong()
        {
            var columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Code", Type = DataType.String, Length = 5 }
            };
            var schema = new TableSchema { Columns = columns };

            var row = new Row();
            row["Code"] = "ABCDEFGHIJK"; // 11 Huruf, padahal max 5

            byte[] bytes = RowSerializer.SerializeRow(schema, row);
            Row result = RowSerializer.DeserializeRow(schema, bytes);

            Assert.Equal("ABCDE", result["Code"]); // Harusnya cuma ambil 5 huruf awal
        }
    }
}