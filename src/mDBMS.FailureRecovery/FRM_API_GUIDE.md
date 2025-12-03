# FRM API Guide

Quick reference for FRM and SM functions.

---

## FRM - Transaction Control (Used by CCM)

### `LogEntry.CreateBeginTransaction(lsn, txnId)`

- Creates BEGIN log entry
- Used by: CCM when starting transaction
- Does NOT auto-flush (call FlushLogBuffer if needed immediately)

### `LogEntry.CreateCommit(lsn, txnId)`

- Creates COMMIT log entry
- Used by: CCM when committing transaction
- Auto-flushes log to disk + triggers periodic checkpoint (every 10 commits)

### `LogEntry.CreateAbort(lsn, txnId)`

- Creates ABORT log entry
- Used by: CCM when aborting transaction
- Auto-flushes log to disk

### `WriteLogEntry(logEntry)`

- Writes control log entry (BEGIN/COMMIT/ABORT/CHECKPOINT)
- Used by: CCM for transaction control
- Difference from WriteLog: this is for control operations, WriteLog is for data operations

### `UndoTransaction(txnId)` -> recovery wajib

- Rolls back all operations for a transaction
- Used by: CCM when aborting or during recovery
- Reads log backwards, reverses operations (DELETE→INSERT, INSERT→DELETE, UPDATE→restore old value)

Recovery bonus system failure (function Recover() belom implemented)

---

## FRM - Data Logging (Used by QP)

### `WriteLog(executionLog)`

- Writes data operation log (INSERT/UPDATE/DELETE)
- Used by: QP after executing data operations
- Auto-flushes only if log buffer reaches 100 entries
- Difference from WriteLogEntry: this is for data operations, WriteLogEntry is for control operations

### `GetCurrentLSN()`

- Returns current log sequence number
- Used by: CCM/QP when creating log entries manually

---

## FRM - Buffer & Checkpoint (Internal/Manual)

### `SaveCheckpoint()`

- Manually triggers checkpoint (flushes all dirty pages + writes CHECKPOINT log)
- Used by: Manual checkpoint or automatic (every 10 commits)

### `FlushLogBuffer()`

- Manually flushes log buffer to disk
- Used by: When immediate durability needed (e.g., testing or critical operations)

### `FlushPageToDisk(page)`

- Manually flushes specific page to disk
- Used by: Checkpoint or eviction

---

## SM - Changes

### `AddBlock(dataWrite)`

- Inserts new row to table
- Params: DataWrite(tableName, columnValues)
- Returns: row.id (GUID string)
- Writes to BUFFER first, NOT disk
- Disk write happens on checkpoint or eviction

### `WriteBlock(dataUpdate)` -> ini update block

- Updates existing row
- Params: DataUpdate(tableName, rowId, newColumnValues)
- Returns: bool (success/failure)
- Writes to BUFFER first, NOT disk

### `DeleteBlock(dataDelete)`

- Deletes row (marks as deleted)
- Params: DataDelete(tableName, rowId)
- Returns: bool (success/failure)
- Writes to BUFFER first, NOT disk

### `ReadBlock(dataRetrieval)`

- Reads rows from table
- Params: DataRetrieval(tableName, columnNames)
- Returns: IEnumerable<Dictionary<string, object>>
- Checks BUFFER first, then disk (buffer-first architecture)

---

## Key Differences

### WriteLog vs WriteLogEntry

- **WriteLog**: For data operations (INSERT/UPDATE/DELETE) from QP
  - Takes ExecutionLog object
  - Auto-flush only on buffer full (100 entries)
- **WriteLogEntry**: For control operations (BEGIN/COMMIT/ABORT) from CCM
  - Takes LogEntry object
  - Auto-flush on COMMIT/ABORT

### Buffer vs Disk

- **Buffer**: In-memory cache (100 pages max, 4KB each)
- **Disk**: Persistent storage (.dat files)
- All writes go to buffer first
- Disk write happens on:
  - Checkpoint (manual or periodic)
  - Eviction (when buffer full, LRU page evicted)

### Dirty Page vs Clean Page

- **Dirty**: Modified in buffer but not yet written to disk
- **Clean**: Synced with disk
- Dirty pages flushed on checkpoint or eviction

### Eviction

- When buffer reaches 100 pages, LRU (Least Recently Used) page is evicted
- If evicted page is dirty, automatically flushed to disk
- Happens automatically inside WriteToBuffer

---

## Auto-Flush Summary

| Function               | Auto-Flush Log?      | Auto-Flush Pages? |
| ---------------------- | -------------------- | ----------------- |
| WriteLog               | Only if buffer ≥ 100 | No                |
| WriteLogEntry (BEGIN)  | Only if buffer ≥ 100 | No                |
| WriteLogEntry (COMMIT) | YES                  | No                |
| WriteLogEntry (ABORT)  | YES                  | No                |
| SaveCheckpoint         | YES                  | YES (all dirty)   |
| Buffer eviction        | No                   | YES (if dirty)    |

---

## File Locations

- **Log**: `logs/mDBMS.log`
- **Data**: `{table}.dat` (e.g., `users.dat`)
- Both relative to executable directory

---

## Integration Summary

**CCM uses**:

- CreateBeginTransaction, CreateCommit, CreateAbort
- WriteLogEntry
- UndoTransaction

**QP uses**:

- WriteLog (with ExecutionLog)
- GetCurrentLSN (optional, for manual log creation)

**SM used by QP**:

- AddBlock (INSERT)
- WriteBlock (UPDATE)
- DeleteBlock (DELETE)
- ReadBlock (SELECT)

**System uses**:

- Recover (on startup after crash)

**Manual/monitoring**:

- SaveCheckpoint
- FlushLogBuffer
- GetDirtyPages
