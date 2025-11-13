using System;

namespace mDBMS.Common.Models.CCM;

/// <summary>
/// Representasi unik tiap objek database yang divalidasi oleh CCM.
/// </summary>
public class DatabaseObject : IEquatable<DatabaseObject>
{
    /// <summary>
    /// Tipe objek database (Table, DiskBlock, Row, Index, Database)
    /// </summary>
    public DatabaseObjectType ObjectType { get; set; }

    /// <summary>
    /// Identifier utama objek.
    /// - Table: nama tabel
    /// - DiskBlock: ID blok disk (contoh: "BLK-1234")
    /// - Row: ID baris unik atau primary key value
    /// - Index: nama indeks
    /// - Database: nama database
    /// </summary>
    public string ObjectId { get; set; } = string.Empty;

    /// <summary>
    /// Identifier konteks tambahan.
    /// - Row: nama tabel yang memiliki row tersebut
    /// - DiskBlock: nama tabel yang menyimpan blok tersebut
    /// - Index: nama tabel yang memiliki indeks tersebut
    /// </summary>
    public string? ContextId { get; set; }

    /// <summary>
    /// Metadata tambahan untuk objek.
    /// Contoh: schema name, partition info, dll.
    /// </summary>
    public string? Metadata { get; set; }

    /// <summary>
    /// Konstruktor default
    /// </summary>
    public DatabaseObject()
    {
    }

    /// <summary>
    /// Konstruktor dengan parameter
    /// </summary>
    public DatabaseObject(DatabaseObjectType objectType, string objectId, string? contextId = null, string? metadata = null)
    {
        ObjectType = objectType;
        ObjectId = objectId ?? throw new ArgumentNullException(nameof(objectId));
        ContextId = contextId;
        Metadata = metadata;
    }

    /// <summary>
    /// Factory method untuk membuat objek tabel
    /// </summary>
    public static DatabaseObject CreateTable(string tableName, string? schemaName = null)
    {
        return new DatabaseObject(DatabaseObjectType.Table, tableName, schemaName);
    }

    /// <summary>
    /// Factory method untuk membuat objek disk block
    /// </summary>
    public static DatabaseObject CreateDiskBlock(string blockId, string? tableName = null)
    {
        return new DatabaseObject(DatabaseObjectType.DiskBlock, blockId, tableName);
    }

    /// <summary>
    /// Factory method untuk membuat objek row
    /// </summary>
    public static DatabaseObject CreateRow(string rowId, string tableName, string? schemaName = null)
    {
        return new DatabaseObject(DatabaseObjectType.Row, rowId, tableName, schemaName);
    }

    /// <summary>
    /// Factory method untuk membuat objek index
    /// </summary>
    public static DatabaseObject CreateIndex(string indexName, string? tableName = null)
    {
        return new DatabaseObject(DatabaseObjectType.Index, indexName, tableName);
    }

    /// <summary>
    /// Factory method untuk membuat objek database
    /// </summary>
    public static DatabaseObject CreateDatabase(string databaseName)
    {
        return new DatabaseObject(DatabaseObjectType.Database, databaseName);
    }

    /// <summary>
    /// Menghasilkan representasi string yang qualified
    /// </summary>
    public string ToQualifiedString()
    {
        var parts = new System.Collections.Generic.List<string>();

        if (!string.IsNullOrEmpty(Metadata))
            parts.Add($"[{Metadata}]");

        if (!string.IsNullOrEmpty(ContextId))
            parts.Add(ContextId);

        parts.Add(ObjectId);

        return $"{ObjectType}:{string.Join(".", parts)}";
    }

    /// <summary>
    /// Untuk debugging
    /// </summary>
    public override string ToString() => ToQualifiedString();

    #region Equality Members

    public bool Equals(DatabaseObject? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return ObjectType == other.ObjectType &&
               ObjectId == other.ObjectId &&
               ContextId == other.ContextId &&
               Metadata == other.Metadata;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as DatabaseObject);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(ObjectType, ObjectId, ContextId, Metadata);
    }

    public static bool operator ==(DatabaseObject? left, DatabaseObject? right)
    {
        return Equals(left, right);
    }

    public static bool operator !=(DatabaseObject? left, DatabaseObject? right)
    {
        return !Equals(left, right);
    }

    #endregion
}
