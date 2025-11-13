namespace mDBMS.Common.Models.CCM;

/// <summary>
/// Enum yang mendefinisikan tipe objek database yang dapat divalidasi oleh CCM.
/// </summary>
public enum DatabaseObjectType
{
    /// <summary>
    /// Objek berupa tabel keseluruhan.
    /// Identifier: nama tabel
    /// </summary>
    Table,

    /// <summary>
    /// Objek berupa blok disk fisik.
    /// Identifier: ID blok disk (misalnya: "BLK-1234")
    /// </summary>
    DiskBlock,

    /// <summary>
    /// Objek berupa baris/row individual dalam tabel.
    /// Identifier: ID baris unik (e.g.: "ROW-5678" atau primary key value)
    /// </summary>
    Row,

    /// <summary>
    /// Objek berupa struktur indeks.
    /// Identifier: nama indeks
    /// </summary>
    Index,

    /// <summary>
    /// Objek berupa database keseluruhan.
    /// Identifier: nama database
    /// </summary>
    Database
}
