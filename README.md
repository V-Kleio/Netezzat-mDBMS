# Netezzat-mDBMS

A multi-user mini Database Management System (mDBMS) built in C# with support for concurrent transactions, query optimization, and failure recovery.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the Server](#running-the-server)
  - [Running the CLI Client](#running-the-cli-client)
  - [Seeding Sample Data](#seeding-sample-data)
- [SQL Support](#sql-support)
  - [Supported Statements](#supported-statements)
  - [Query Examples](#query-examples)
- [System Components](#system-components)
  - [Query Optimizer](#query-optimizer)
  - [Query Processor](#query-processor)
  - [Concurrency Control](#concurrency-control)
  - [Storage Manager](#storage-manager)
  - [Failure Recovery](#failure-recovery)
- [Architecture Decisions](#architecture-decisions)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Netezzat-mDBMS is an educational implementation of a relational database management system with focus on:

- **Multi-user concurrency** using concurrency manager
- **Query optimization** with cost-based and heuristic approaches
- **ACID transactions** with write-ahead logging (WAL)
- **Client-server architecture** with TCP/IP communication
- **SQL query support** (SELECT, INSERT, UPDATE, DELETE, JOIN operations)

This project demonstrates core DBMS concepts including query parsing, optimization, execution, transaction management, and crash recovery.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         CLI Client                          │
│                    (mDBMS.CLI)                              │
└────────────────────────┬────────────────────────────────────┘
                         │ TCP/IP
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Server (mDBMS.Server)                  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │        Query Processor (mDBMS.QueryProcessor)       │    │
│  │  ┌───────────────────────────────────────────────┐  │    │
│  │  │   Query Optimizer (mDBMS.QueryOptimizer)      │  │    │
│  │  │   • SQL Parser (Lexer + Parser)               │  │    │
│  │  │   • Plan Builder (Heuristic Optimization)     │  │    │
│  │  │   • Cost Model (I/O + CPU estimation)         │  │    │
│  │  │   • Plan Cache (Query result caching)         │  │    │
│  │  └───────────────────────────────────────────────┘  │    │
│  │  ┌───────────────────────────────────────────────┐  │    │
│  │  │   Execution Engine (DML Operators)            │  │    │
│  │  │   • TableScan, IndexScan, IndexSeek           │  │    │
│  │  │   • NestedLoop/Hash/Merge Join                │  │    │
│  │  │   • Filter, Project, Aggregate, Sort          │  │    │
│  │  │   • Insert, Update, Delete                    │  │    │
│  │  └───────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Concurrency Control (mDBMS.ConcurrencyControl)      │    │
│  │   • Two-Phase Locking (2PL)                         │    │
│  │   • Deadlock Detection                              │    │
│  │   • Transaction Manager                             │    │
│  └─────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Failure Recovery (mDBMS.FailureRecovery)            │    │
│  │   • Write-Ahead Logging (WAL)                       │    │
│  │   • REDO/UNDO Recovery                              │    │
│  │   • Checkpoint Management                           │    │
│  └─────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Storage Manager (mDBMS.StorageManager)              │    │
│  │   • Page-based Storage (4KB blocks)                 │    │
│  │   • Buffer Manager (LRU caching)                    │    │
│  │   • Hash Index (in-memory)                          │    │
│  │   • Data Serialization                              │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## Features

### Core Database Features
- **ACID Transactions** - Atomicity, Consistency, Isolation, Durability
- **SQL Query Support** - SELECT, INSERT, UPDATE, DELETE with WHERE, JOIN, GROUP BY, ORDER BY
- **Multiple Join Types** - INNER, LEFT, RIGHT, FULL OUTER, CROSS JOIN
- **Join Algorithms** - Nested Loop, Hash Join, Merge Join
- **Aggregation** - SUM, COUNT, AVG, MIN, MAX with GROUP BY
- **Indexing** - Hash-based indexes for fast lookups
- **Query Optimization** - Cost-based and heuristic query planning

### Concurrency & Recovery
- **Two-Phase Locking (2PL)** - Prevents conflicts in concurrent transactions
- **Deadlock Detection** - Timeout-based detection and resolution
- **Write-Ahead Logging** - Ensures durability and crash recovery
- **REDO/UNDO Recovery** - Automatic recovery after crashes
- **Multi-threaded Server** - Handles multiple concurrent clients

### Performance
- **Buffer Manager** - LRU-based page caching
- **Query Plan Caching** - Reuses optimized plans for repeated queries
- **Predicate Pushdown** - Filters data as early as possible
- **Join Reordering** - Optimizes join order based on selectivity

---

## Project Structure

```
Netezzat-mDBMS/
├── src/
│   ├── mDBMS.CLI/                    # Command-line interface client
│   │   └── Program.cs                # CLI entry point
│   ├── mDBMS.Server/                 # TCP/IP server
│   │   └── Program.cs                # Server entry point
│   ├── mDBMS.Common/                 # Shared data structures
│   │   ├── Data/                     # Row, Page, Statistic, Condition
│   │   ├── QueryData/                # Query, PlanNode, ExecutionResult
│   │   ├── Transaction/              # Action, TransactionState
│   │   ├── Interfaces/               # IStorageManager, IBufferManager, etc.
│   │   └── Net/                      # Network encoding/decoding
│   ├── mDBMS.QueryOptimizer/         # Query optimization engine
│   │   ├── SqlLexer.cs               # SQL tokenization
│   │   ├── SqlParser.cs              # SQL parsing
│   │   ├── PlanBuilder.cs            # Query plan construction
│   │   ├── QueryOptimizerEngine.cs   # Main optimizer orchestrator
│   │   ├── QueryRewriter.cs          # Query transformation rules
│   │   ├── QueryPlanCache.cs         # Plan caching
│   │   └── Core/
│   │       ├── ICostModel.cs         # Cost estimation interface
│   │       └── SimpleCostModel.cs    # I/O + CPU cost model
│   ├── mDBMS.QueryProcessor/         # Query execution engine
│   │   ├── QueryProcessor.cs         # Main execution coordinator
│   │   └── DML/                      # Data manipulation operators
│   │       ├── TableScanOperator.cs
│   │       ├── IndexScanOperator.cs
│   │       ├── IndexSeekOperator.cs
│   │       ├── FilterOperator.cs
│   │       ├── ProjectOperator.cs
│   │       ├── JoinOperator.cs       # CrossJoin dispatcher
│   │       ├── NestedLoopJoinOperator.cs
│   │       ├── HashJoinOperator.cs
│   │       ├── MergeJoinOperator.cs
│   │       ├── AggregateOperator.cs
│   │       ├── SortOperator.cs
│   │       ├── InsertOperator.cs
│   │       ├── UpdateOperator.cs
│   │       └── DeleteOperator.cs
│   ├── mDBMS.ConcurrencyControl/     # Transaction & locking
│   │   ├── ConcurrencyControlManager.cs
│   │   ├── TransactionManager.cs
│   │   └── LockManager.cs
│   ├── mDBMS.FailureRecovery/        # WAL & recovery
│   │   ├── FailureRecoveryManager.cs
│   │   ├── LogManager.cs
│   │   └── RecoveryManager.cs
│   ├── mDBMS.StorageManager/         # Disk & buffer management
│   │   ├── StorageEngine.cs          # Page I/O
│   │   ├── BufferManager.cs          # LRU page cache
│   │   ├── HashIndex.cs              # In-memory indexing
│   │   └── Seeder.cs                 # Sample data generator
│   └── mDBMS.Seeder/                 # Standalone seeder app
│       └── Program.cs                # Generates test data
├── data/                             # Database files (*.dat, mDBMS.log)
│   ├── students.dat
│   ├── courses.dat
│   ├── enrollments.dat
│   ├── departments.dat
│   ├── instructors.dat
│   └── mDBMS.log
├── tests/                            # Unit & integration tests
└── Netezzat-mDBMS.sln               # Visual Studio solution
```

---

## Getting Started

### Prerequisites

- **.NET 8.0 SDK** or higher
- **Windows/Linux/macOS** (cross-platform)
- **Visual Studio 2022** or **VS Code** (optional)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/Netezzat-mDBMS.git
   cd Netezzat-mDBMS
   ```

2. **Restore dependencies:**
   ```bash
   dotnet restore
   ```

3. **Build the solution:**
   ```bash
   dotnet build
   ```

### Running the Server

1. **Start the server:**
   ```bash
   cd src/mDBMS.Server
   dotnet run
   ```

   **Optional arguments:**
   - `--port <port>` or `-p <port>` - Set server port (default: 5761)
   - `--ccm-strategy <ccm protocol>` or `-s <ccm protocol>` - Set the ccm protocol (default: 2PL)

   **Example:**
   ```bash
   dotnet run --port 8080 -s 2PL
   ```

   **Expected output:**
   ```
   [Server] Initializing components...
   [StorageEngine] DataPath set at: C:\path\to\Netezzat-mDBMS\data
   [FailureRecoveryManager] Initialized with WAL at: C:\path\to\data\mDBMS.log
   [2PL] ConcurrencyControlManager initialized
   [Server] Listening on 127.0.0.1:5761
   [Server] Ready to accept connections...
   ```

### Running the CLI Client

1. **Open a new terminal and start the CLI:**
   ```bash
   cd src/mDBMS.CLI
   dotnet run
   ```

   **Optional arguments:**
   - `--host <hostname>` or `-h <hostname>` - Server hostname (default: localhost)
   - `--port <port>` or `-p <port>` - Server port (default: 5761)

   **Example:**
   ```bash
   dotnet run --host 192.168.1.100 --port 8080
   ```

   **Expected output:**
   ```
   mDBMS CLI siap digunakan. Ketik EXIT untuk keluar.
   mDBMS >
   ```

2. **Execute SQL queries:**
   ```sql
   mDBMS > SELECT * FROM students WHERE GPA > 3.5
   ```

### Seeding Sample Data

1. **Run the seeder to generate test data:**
   ```bash
   cd src/mDBMS.Seeder
   dotnet run
   ```

   **Expected output:**
   ```
   ==============================================
   mDBMS Database Seeder
   ==============================================
   Target Directory: C:\path\to\Netezzat-mDBMS\data

   Created data directory
   Cleaned up existing files
   Students: 100 rows -> students.dat
   Courses: 50 rows -> courses.dat
   Enrollments: 300 rows -> enrollments.dat
   Departments: 5 rows -> departments.dat
   Instructors: 20 rows -> instructors.dat

   ==============================================
   Seeder completed successfully!
   ==============================================
   ```

2. **Sample tables created:**
   - **Students** (100 rows) - StudentID, FullName, Email, Age, GPA, DepartmentID
   - **Courses** (50 rows) - CourseID, CourseName, Credits, InstructorID
   - **Enrollments** (300 rows) - EnrollmentID, StudentID, CourseID, Grade, EnrollmentDate
   - **Departments** (5 rows) - DepartmentID, DepartmentName, Building, Budget
   - **Instructors** (20 rows) - InstructorID, FullName, Email, DepartmentID, Salary

---

## SQL Support

### Supported Statements

#### SELECT Queries
```sql
-- Basic SELECT
SELECT * FROM students;
SELECT FullName, GPA FROM students;

-- WHERE clause
SELECT * FROM students WHERE GPA > 3.5;
SELECT * FROM students WHERE Age >= 20 AND DepartmentID = 1;

-- JOIN operations
SELECT s.FullName, c.CourseName, e.Grade
FROM students s
JOIN enrollments e ON s.StudentID = e.StudentID
JOIN courses c ON e.CourseID = c.CourseID;

-- CROSS JOIN (Cartesian product)
SELECT * FROM students, courses;
SELECT * FROM students CROSS JOIN courses;

-- Implicit JOIN (comma-separated tables with WHERE)
SELECT * FROM students s, enrollments e
WHERE s.StudentID = e.StudentID;

-- ORDER BY
SELECT * FROM students ORDER BY GPA DESC;
SELECT FullName, Age FROM students ORDER BY Age ASC, FullName DESC;

-- Combined query
SELECT d.DepartmentName, AVG(s.GPA) as AvgGPA
FROM students s
JOIN departments d ON s.DepartmentID = d.DepartmentID
WHERE s.Age >= 20
ORDER BY AvgGPA DESC;
```

#### INSERT Statements
```sql
-- Insert single row
INSERT INTO students (StudentID, FullName, Email, Age, GPA, DepartmentID)
VALUES (101, 'John Doe', 'john@example.com', 22, 3.75, 1);

-- Insert multiple rows
INSERT INTO students (StudentID, FullName, Email, Age, GPA, DepartmentID)
VALUES
  (102, 'Jane Smith', 'jane@example.com', 21, 3.85, 2),
  (103, 'Bob Johnson', 'bob@example.com', 23, 3.65, 1);

-- Insert from SELECT
INSERT INTO archived_students
SELECT * FROM students WHERE GPA < 2.0;
```

#### UPDATE Statements
```sql
-- Update single column
UPDATE students SET GPA = 3.8 WHERE StudentID = 101;

-- Update multiple columns
UPDATE students
SET GPA = 3.9, Email = 'newemail@example.com'
WHERE StudentID = 102;
```

#### DELETE Statements
```sql
-- Delete with condition
DELETE FROM enrollments WHERE Grade < 2.0;

-- Delete all rows (use with caution!)
DELETE FROM temp_table;
```

### Query Examples

#### Top Students by Department
```sql
SELECT d.DepartmentName, s.FullName, s.GPA
FROM students s
JOIN departments d ON s.DepartmentID = d.DepartmentID
WHERE s.GPA > 3.5
ORDER BY d.DepartmentName, s.GPA DESC;
```

---

## System Components

### Query Optimizer

**Location:** `src/mDBMS.QueryOptimizer/`

**Responsibilities:**
- Parse SQL into structured query objects
- Generate optimized execution plans
- Estimate query costs
- Cache compiled plans

**Key Classes:**
- `SqlLexer` - Tokenizes SQL strings
- `SqlParser` - Parses tokens into Query objects
- `PlanBuilder` - Constructs query execution trees using heuristics
- `QueryOptimizerEngine` - Orchestrates optimization pipeline
- `SimpleCostModel` - Estimates I/O and CPU costs

**Optimization Techniques:**
- **Predicate Pushdown** - Moves WHERE filters close to table scans
- **Join Reordering** - Selects optimal join sequence
- **Algorithm Selection** - Chooses best join algorithm (nested loop/hash/merge)
- **Early Projection** - Projects columns as early as possible
- **Index Selection** - Uses indexes when available

---

### Query Processor

**Location:** `src/mDBMS.QueryProcessor/`

**Responsibilities:**
- Execute optimized query plans
- Coordinate with storage, CCM, and FRM
- Implement relational operators

**Key Operators:**
- **Scan Operators** - TableScan, IndexScan, IndexSeek
- **Join Operators** - NestedLoop, Hash, Merge, Cross
- **Filter/Project** - Predicate evaluation, column projection
- **Aggregate** - SUM, COUNT, AVG, MIN, MAX with GROUP BY
- **Sort** - ORDER BY implementation
- **DML** - Insert, Update, Delete with transaction support

**Execution Model:**
- **Iterator Model** - Pull-based execution with `IEnumerable<Row>`
- **Volcano-style** - Each operator implements `Execute()` method
- **Lazy Evaluation** - Rows processed on-demand

---

### Concurrency Control

**Location:** `src/mDBMS.ConcurrencyControl/`

**Responsibilities:**
- Ensure transaction isolation (ACID)
- Prevent conflicts between concurrent transactions
- Detect and resolve deadlocks

**Implementation:**
- **Two-Phase Locking (2PL)** - Growing phase (acquire locks) → Shrinking phase (release locks)
- **Lock Types** - Shared (read) and Exclusive (write) locks
- **Lock Granularity** - Row-level locking
- **Deadlock Detection** - Timeout-based detection (5 seconds default)
- **Transaction States** - Active → PartiallyCommitted → Committed/Aborted → Terminated

**Usage:**
```csharp
// Validate read action
var action = new Action(Action.ActionType.Read, DatabaseObject.CreateRow(rowId, tableName), txId, query);
var result = ccm.ValidateAction(action);

if (result.IsValid)
{
    // Proceed with operation
}
else
{
    // Abort transaction
}
```

---

### Storage Manager

**Location:** `src/mDBMS.StorageManager/`

**Responsibilities:**
- Persist data to disk
- Manage buffer pool
- Provide index structures

**Features:**
- **Page-based Storage** - 4KB blocks with file header
- **Binary Serialization** - Efficient row encoding
- **Buffer Manager** - LRU-based page caching (configurable size)
- **Hash Index** - In-memory B-tree alternative
- **Schema Metadata** - Table and column definitions

**File Format:**
```
┌─────────────────────────────────────┐
│  File Header (4KB)                  │
│  • Magic Number                     │
│  • Version                          │
│  • Table Metadata                   │
│  • Index Metadata                   │
├─────────────────────────────────────┤
│  Block 0 (4KB)                      │
│  • Row Count                        │
│  • Row Data (serialized)            │
├─────────────────────────────────────┤
│  Block 1 (4KB)                      │
├─────────────────────────────────────┤
│  ...                                │
└─────────────────────────────────────┘
```

---

### Failure Recovery

**Location:** `src/mDBMS.FailureRecovery/`

**Responsibilities:**
- Ensure durability (ACID)
- Recover from crashes
- Maintain transaction logs

**Implementation:**
- **Write-Ahead Logging (WAL)** - Log changes before modifying data
- **REDO Logging** - Replay committed transactions after crash
- **UNDO Logging** - Rollback aborted transactions
- **Checkpointing** - Periodic log truncation
- **Buffer Integration** - Coordinates with buffer manager for dirty page writes

**Log Entry Format:**
```csharp
public class LogEntry
{
    public int TransactionId { get; set; }
    public LogEntryType Type { get; set; }  // BEGIN, COMMIT, ABORT, UPDATE, INSERT, DELETE
    public string TableName { get; set; }
    public string RowIdentifier { get; set; }
    public Row? BeforeImage { get; set; }  // For UNDO
    public Row? AfterImage { get; set; }   // For REDO
    public DateTime Timestamp { get; set; }
}
```

**Recovery Algorithm:**
1. Read log from last checkpoint
2. REDO all committed transactions
3. UNDO all incomplete transactions
4. Rebuild buffer and indexes

---

## Architecture Decisions

### 1. **Separation of Concerns**
Each component has a single, well-defined responsibility:
- **Optimizer** - Plan generation
- **Processor** - Plan execution
- **CCM** - Transaction isolation
- **FRM** - Durability & recovery
- **Storage** - Persistence

### 2. **Iterator Model for Execution**
- Operators implement `IEnumerable<Row>`
- Supports pipelining and lazy evaluation
- Memory-efficient for large result sets

### 3. **Cost-Based Optimization**
- Uses table statistics (row count, distinct values)
- Estimates I/O and CPU costs
- Chooses optimal join algorithms dynamically

### 4. **Two-Phase Locking (2PL)**
- Ensures serializability
- Simple to implement and reason about
- Trade-off: potential for deadlocks (handled via timeouts)

### 5. **Write-Ahead Logging**
- Industry-standard durability mechanism
- Enables crash recovery
- Supports both REDO and UNDO operations

### 6. **Client-Server Architecture**
- Centralized data management
- Multi-client support
- Network protocol abstraction (easy to add HTTP/gRPC later)

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
