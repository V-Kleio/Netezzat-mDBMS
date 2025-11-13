namespace mDBMS.Common.Models.CCM;

/// <summary>
/// Enum yang mendefinisikan tipe aksi yang dapat dilakukan pada objek database.
/// </summary>
public enum ActionType
{
    /// <summary>
    /// Operasi (SELECT, Read)
    /// </summary>
    Read,

    /// <summary>
    /// Operasi (INSERT, UPDATE, DELETE)
    /// </summary>
    Write
}
