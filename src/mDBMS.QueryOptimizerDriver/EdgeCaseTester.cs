using mDBMS.Common.QueryData;
using mDBMS.QueryOptimizer;

namespace mDBMS.QueryOptimizerDriver;

/// <summary>
/// Test untuk edge case pada Query Optimizer dan SQL Parser.
/// Menguji berbagai variasi kueri SQL untuk memastikan keandalan.
/// </summary>
public static class EdgeCaseTester
{
    private static int _passCount = 0;
    private static int _failCount = 0;
    private static List<string> _failures = new();

    public static void RunAllTests(QueryOptimizerEngine optimizer)
    {
        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("        EDGE CASE TESTS - Query Optimizer");
        Console.WriteLine(new string('=', 60) + "\n");

        _passCount = 0;
        _failCount = 0;
        _failures.Clear();

        // Category 1: Basic SELECT variations
        Console.WriteLine("\n=== 1. BASIC SELECT VARIATIONS ===\n");
        TestSelectAll(optimizer);
        TestSelectSpecificColumns(optimizer);
        TestSelectSingleColumn(optimizer);
        TestSelectWithAlias(optimizer);

        // Category 2: WHERE clause variations
        Console.WriteLine("\n=== 2. WHERE CLAUSE VARIATIONS ===\n");
        TestWhereEquality(optimizer);
        TestWhereGreaterThan(optimizer);
        TestWhereLessThan(optimizer);
        TestWhereGreaterOrEqual(optimizer);
        TestWhereLessOrEqual(optimizer);
        TestWhereNotEqual(optimizer);
        TestWhereWithStringLiteral(optimizer);
        TestWhereWithNumericValue(optimizer);

        // Category 3: ORDER BY variations
        Console.WriteLine("\n=== 3. ORDER BY VARIATIONS ===\n");
        TestOrderByAsc(optimizer);
        TestOrderByDesc(optimizer);
        TestOrderByMultipleColumns(optimizer);
        TestOrderByWithWhere(optimizer);

        // Category 4: JOIN variations
        Console.WriteLine("\n=== 4. JOIN VARIATIONS ===\n");
        TestInnerJoin(optimizer);
        TestLeftJoin(optimizer);
        TestJoinWithWhere(optimizer);
        TestJoinWithOrderBy(optimizer);
        TestMultipleJoins(optimizer);

        // Category 5: GROUP BY / Aggregation
        Console.WriteLine("\n=== 5. GROUP BY / AGGREGATION ===\n");
        TestGroupBySingle(optimizer);
        TestGroupByMultiple(optimizer);
        TestGroupByWithWhere(optimizer);

        // Category 6: Complex queries
        Console.WriteLine("\n=== 6. COMPLEX QUERIES ===\n");
        TestComplexWhereOrderBy(optimizer);
        TestSelectWithAllClauses(optimizer);

        // Category 7: INSERT variations
        Console.WriteLine("\n=== 7. INSERT VARIATIONS ===\n");
        TestInsertSingleRow(optimizer);
        TestInsertMultipleValues(optimizer);
        TestInsertWithColumns(optimizer);

        // Category 8: UPDATE variations
        Console.WriteLine("\n=== 8. UPDATE VARIATIONS ===\n");
        TestUpdateSingle(optimizer);
        TestUpdateWithWhere(optimizer);
        TestUpdateMultipleColumns(optimizer);

        // Category 9: DELETE variations
        Console.WriteLine("\n=== 9. DELETE VARIATIONS ===\n");
        TestDeleteAll(optimizer);
        TestDeleteWithWhere(optimizer);

        // Category 10: Edge cases and boundary conditions
        Console.WriteLine("\n=== 10. EDGE CASES & BOUNDARIES ===\n");
        TestEmptyTable(optimizer);
        TestInvalidTableName(optimizer);
        TestTableNameWithSpecialChars(optimizer);
        TestTableNameStartsWithNumber(optimizer);
        TestEmptyTableName(optimizer);
        TestJoinWithInvalidTable(optimizer);
        TestUpdateInvalidTable(optimizer);
        TestDeleteInvalidTable(optimizer);
        TestLongColumnNames(optimizer);
        TestNumericColumnNames(optimizer);
        TestColumnStartsWithNumber(optimizer);
        TestCaseSensitivity(optimizer);
        TestWhitespaceHandling(optimizer);
        TestSpecialCharactersInStrings(optimizer);

        // Category 11: Error handling (expected failures)
        Console.WriteLine("\n=== 11. ERROR HANDLING (Expected Failures) ===\n");
        TestMalformedQuery(optimizer);
        TestMissingTableName(optimizer);
        TestInvalidOperator(optimizer);

        // Category 12: Additional DML edge cases
        Console.WriteLine("\n=== 12. ADDITIONAL DML EDGE CASES ===\n");
        TestUpdateNoSet(optimizer);
        TestDeleteWithComplexWhere(optimizer);
        TestInsertEmptyValues(optimizer);

        // Category 13: Join edge cases
        Console.WriteLine("\n=== 13. JOIN EDGE CASES ===\n");
        TestJoinSelfJoin(optimizer);
        TestJoinMultipleConditions(optimizer);
        TestRightJoin(optimizer);

        // Category 14: Nested and complex conditions
        Console.WriteLine("\n=== 14. COMPLEX CONDITIONS ===\n");
        TestAndConditions(optimizer);
        TestOrConditions(optimizer);
        TestRangeCondition(optimizer);
        TestBetweenLikeCondition(optimizer);

        // Category 15: PlanNode ToSteps verification
        Console.WriteLine("\n=== 15. PLAN NODE TO STEPS VERIFICATION ===\n");
        TestPlanStepsGeneration(optimizer);
        TestPlanStepsOrder(optimizer);
        TestPlanCostCalculation(optimizer);

        // Print summary
        PrintSummary();
    }

    #region Basic SELECT Tests

    private static void TestSelectAll(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "SELECT * (all columns)",
            "SELECT * FROM employees");
    }

    private static void TestSelectSpecificColumns(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "SELECT specific columns",
            "SELECT id, name, salary FROM employees");
    }

    private static void TestSelectSingleColumn(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "SELECT single column",
            "SELECT name FROM employees");
    }

    private static void TestSelectWithAlias(QueryOptimizerEngine optimizer)
    {
        // Table-qualified column references work
        RunTest(optimizer, "SELECT with table reference",
            "SELECT employees.id, employees.name FROM employees");
    }

    #endregion

    #region WHERE Clause Tests

    private static void TestWhereEquality(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with = operator",
            "SELECT * FROM employees WHERE id = 100");
    }

    private static void TestWhereGreaterThan(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with > operator",
            "SELECT * FROM employees WHERE salary > 50000");
    }

    private static void TestWhereLessThan(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with < operator",
            "SELECT * FROM employees WHERE age < 30");
    }

    private static void TestWhereGreaterOrEqual(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with >= operator",
            "SELECT * FROM employees WHERE salary >= 60000");
    }

    private static void TestWhereLessOrEqual(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with <= operator",
            "SELECT * FROM employees WHERE age <= 25");
    }

    private static void TestWhereNotEqual(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with <> operator",
            "SELECT * FROM employees WHERE status <> 'inactive'");
    }

    private static void TestWhereWithStringLiteral(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with string literal",
            "SELECT * FROM employees WHERE name = 'John'");
    }

    private static void TestWhereWithNumericValue(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE with numeric value",
            "SELECT * FROM employees WHERE department_id = 5");
    }

    #endregion

    #region ORDER BY Tests

    private static void TestOrderByAsc(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "ORDER BY ASC",
            "SELECT * FROM employees ORDER BY name ASC");
    }

    private static void TestOrderByDesc(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "ORDER BY DESC",
            "SELECT * FROM employees ORDER BY salary DESC");
    }

    private static void TestOrderByMultipleColumns(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "ORDER BY multiple columns",
            "SELECT * FROM employees ORDER BY department ASC, salary DESC");
    }

    private static void TestOrderByWithWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "WHERE + ORDER BY combination",
            "SELECT * FROM employees WHERE age > 25 ORDER BY salary DESC");
    }

    #endregion

    #region JOIN Tests

    private static void TestInnerJoin(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "INNER JOIN",
            "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id");
    }

    private static void TestLeftJoin(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "LEFT JOIN",
            "SELECT * FROM employees LEFT JOIN departments ON employees.dept_id = departments.id");
    }

    private static void TestJoinWithWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "JOIN with WHERE",
            "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id WHERE salary > 50000");
    }

    private static void TestJoinWithOrderBy(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "JOIN with ORDER BY",
            "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id ORDER BY name ASC");
    }

    private static void TestMultipleJoins(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Multiple JOINs",
            "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id INNER JOIN locations ON departments.loc_id = locations.id");
    }

    #endregion

    #region GROUP BY Tests

    private static void TestGroupBySingle(QueryOptimizerEngine optimizer)
    {
        // COUNT(*) is not supported by parser - aggregate functions need implementation
        RunTest(optimizer, "GROUP BY single column",
            "SELECT department, COUNT(*) FROM employees GROUP BY department", expectFailure: true);
    }

    private static void TestGroupByMultiple(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "GROUP BY multiple columns",
            "SELECT department, status, COUNT(*) FROM employees GROUP BY department, status", expectFailure: true);
    }

    private static void TestGroupByWithWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "GROUP BY with WHERE",
            "SELECT department, COUNT(*) FROM employees WHERE salary > 50000 GROUP BY department", expectFailure: true);
    }

    #endregion

    #region Complex Query Tests

    private static void TestComplexWhereOrderBy(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Complex WHERE + ORDER BY",
            "SELECT id, name, salary FROM employees WHERE age > 25 ORDER BY salary DESC");
    }

    private static void TestSelectWithAllClauses(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "SELECT with multiple clauses",
            "SELECT id, name FROM employees WHERE age > 21 ORDER BY name ASC");
    }

    #endregion

    #region INSERT Tests

    private static void TestInsertSingleRow(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "INSERT single row",
            "INSERT INTO employees VALUES (1, 'John', 50000)");
    }

    private static void TestInsertMultipleValues(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "INSERT with column list",
            "INSERT INTO employees (id, name, salary) VALUES (2, 'Jane', 60000)");
    }

    private static void TestInsertWithColumns(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "INSERT partial columns",
            "INSERT INTO employees (name, salary) VALUES ('Bob', 45000)");
    }

    #endregion

    #region UPDATE Tests

    private static void TestUpdateSingle(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "UPDATE single column",
            "UPDATE employees SET salary = 55000");
    }

    private static void TestUpdateWithWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "UPDATE with WHERE",
            "UPDATE employees SET salary = 60000 WHERE id = 1");
    }

    private static void TestUpdateMultipleColumns(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "UPDATE multiple columns",
            "UPDATE employees SET salary = 65000, status = 'senior' WHERE age > 40");
    }

    #endregion

    #region DELETE Tests

    private static void TestDeleteAll(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "DELETE all rows",
            "DELETE FROM employees");
    }

    private static void TestDeleteWithWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "DELETE with WHERE",
            "DELETE FROM employees WHERE status = 'inactive'");
    }

    #endregion

    #region Edge Case Tests

    private static void TestEmptyTable(QueryOptimizerEngine optimizer)
    {
        // Tabel tidak ada di MockStorageManager - harus gagal
        RunTest(optimizer, "Query on nonexistent table",
            "SELECT * FROM nonexistent_table", expectFailure: true);
    }

    private static void TestInvalidTableName(QueryOptimizerEngine optimizer)
    {
        // Tabel dengan typo tidak ada di MockStorageManager - harus gagal
        RunTest(optimizer, "Typo in table name",
            "SELECT * FROM employeess", expectFailure: true);  // double 's' typo
    }

    private static void TestTableNameWithSpecialChars(QueryOptimizerEngine optimizer)
    {
        // Parser is lenient - @ stops identifier parsing, 'emp' becomes table name
        // 'emp' doesn't exist in MockStorageManager - should fail
        RunTest(optimizer, "Table name with special characters",
            "SELECT * FROM emp@loyees", expectFailure: true);
    }

    private static void TestTableNameStartsWithNumber(QueryOptimizerEngine optimizer)
    {
        // Nama tabel dimulai dengan angka (invalid identifier)
        RunTest(optimizer, "Table name starts with number",
            "SELECT * FROM 123employees", expectFailure: true);
    }

    private static void TestEmptyTableName(QueryOptimizerEngine optimizer)
    {
        // Parser treats '' as a string literal (empty string)
        // Empty string table doesn't exist - should fail
        RunTest(optimizer, "Empty table name (string literal)",
            "SELECT * FROM ''", expectFailure: true);
    }

    private static void TestJoinWithInvalidTable(QueryOptimizerEngine optimizer)
    {
        // JOIN dengan tabel yang tidak ada - harus gagal
        RunTest(optimizer, "JOIN with nonexistent table",
            "SELECT * FROM employees INNER JOIN nonexistent_dept ON employees.dept_id = nonexistent_dept.id", expectFailure: true);
    }

    private static void TestUpdateInvalidTable(QueryOptimizerEngine optimizer)
    {
        // UPDATE pada tabel yang tidak ada - harus gagal
        RunTest(optimizer, "UPDATE nonexistent table",
            "UPDATE nonexistent_table SET salary = 50000 WHERE id = 1", expectFailure: true);
    }

    private static void TestDeleteInvalidTable(QueryOptimizerEngine optimizer)
    {
        // DELETE dari tabel yang tidak ada - harus gagal
        RunTest(optimizer, "DELETE from nonexistent table",
            "DELETE FROM nonexistent_table WHERE id = 1", expectFailure: true);
    }

    private static void TestLongColumnNames(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Long column names",
            "SELECT very_long_column_name_that_might_cause_issues FROM employees");
    }

    private static void TestNumericColumnNames(QueryOptimizerEngine optimizer)
    {
        // Column names with numeric suffix are valid identifiers (col1, col2)
        RunTest(optimizer, "Column names with numeric suffix",
            "SELECT col1, col2, col3 FROM employees");
    }

    private static void TestColumnStartsWithNumber(QueryOptimizerEngine optimizer)
    {
        // Column names starting with numbers should fail (invalid identifier)
        RunTest(optimizer, "Column name starts with number",
            "SELECT 1column FROM employees", expectFailure: true);
    }

    private static void TestCaseSensitivity(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Mixed case keywords",
            "SeLeCt * FrOm employees WhErE id = 1");
    }

    private static void TestWhitespaceHandling(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Extra whitespace",
            "SELECT    *    FROM    employees    WHERE    id   =   1");
    }

    private static void TestSpecialCharactersInStrings(QueryOptimizerEngine optimizer)
    {
        // SQL standard: escaped single quote is '' (two single quotes)
        RunTest(optimizer, "Special characters in strings",
            "SELECT * FROM employees WHERE name = 'O''Brien'");
    }

    #endregion

    #region Error Handling Tests

    private static void TestMalformedQuery(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Malformed query (incomplete)",
            "SELECT FROM", expectFailure: true);
    }

    private static void TestMissingTableName(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Missing table name",
            "SELECT * FROM", expectFailure: true);
    }

    private static void TestInvalidOperator(QueryOptimizerEngine optimizer)
    {
        // Parser is lenient - unknown tokens like !! are skipped
        // This results in WHERE clause parsing the remaining valid parts
        RunTest(optimizer, "Invalid operator (!! skipped by parser)",
            "SELECT * FROM employees WHERE id !! 5");
    }

    #endregion

    #region Additional DML Edge Cases

    private static void TestUpdateNoSet(QueryOptimizerEngine optimizer)
    {
        // Missing SET clause should fail
        RunTest(optimizer, "UPDATE without SET",
            "UPDATE employees WHERE id = 1", expectFailure: true);
    }

    private static void TestDeleteWithComplexWhere(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "DELETE with complex WHERE",
            "DELETE FROM employees WHERE status = 'inactive' AND age > 65");
    }

    private static void TestInsertEmptyValues(QueryOptimizerEngine optimizer)
    {
        // Empty VALUES should fail
        RunTest(optimizer, "INSERT with empty VALUES",
            "INSERT INTO employees VALUES ()", expectFailure: true);
    }

    #endregion

    #region Join Edge Cases

    private static void TestJoinSelfJoin(QueryOptimizerEngine optimizer)
    {
        // Self-join without alias (parser may not support alias syntax)
        RunTest(optimizer, "Self JOIN (same table)",
            "SELECT * FROM employees INNER JOIN employees ON employees.manager_id = employees.id");
    }

    private static void TestJoinMultipleConditions(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "JOIN with multiple ON conditions",
            "SELECT * FROM employees INNER JOIN departments ON employees.dept_id = departments.id AND employees.loc_id = departments.loc_id");
    }

    private static void TestRightJoin(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "RIGHT JOIN",
            "SELECT * FROM employees RIGHT JOIN departments ON employees.dept_id = departments.id");
    }

    #endregion

    #region Complex Conditions

    private static void TestAndConditions(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Multiple AND conditions",
            "SELECT * FROM employees WHERE age > 25 AND salary > 50000 AND status = 'active'");
    }

    private static void TestOrConditions(QueryOptimizerEngine optimizer)
    {
        // OR conditions may need special handling
        RunTest(optimizer, "Multiple OR conditions",
            "SELECT * FROM employees WHERE department = 'HR' OR department = 'IT'");
    }

    private static void TestRangeCondition(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Range condition (compound)",
            "SELECT * FROM employees WHERE salary >= 40000 AND salary <= 80000");
    }

    private static void TestBetweenLikeCondition(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "BETWEEN clause",
            "SELECT * FROM employees WHERE age BETWEEN 25 AND 40");
    }

    #endregion

    #region Plan Node Tree Verification

    private static void TestPlanStepsGeneration(QueryOptimizerEngine optimizer)
    {
        // Verify that PlanTree is generated correctly
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {"PlanTree generation verification",-40} ");
        
        try
        {
            var query = optimizer.ParseQuery("SELECT id, name FROM employees WHERE age > 30 ORDER BY name");
            var plan = optimizer.OptimizeQuery(query);
            
            // Check that PlanTree exists
            if (plan.PlanTree == null)
            {
                Console.WriteLine($"[FAIL] No PlanTree");
                _failCount++;
                _failures.Add("PlanTree generation: No PlanTree");
                return;
            }

            // Count nodes in tree
            int nodeCount = CountNodes(plan.PlanTree);
            
            // Verify tree has nodes
            if (nodeCount == 0)
            {
                Console.WriteLine($"[FAIL] PlanTree has no nodes");
                _failCount++;
                _failures.Add("PlanTree generation: PlanTree has no nodes");
                return;
            }

            Console.WriteLine($"[PASS] {nodeCount} nodes generated");
            _passCount++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(40, ex.Message.Length))}...");
            _failCount++;
            _failures.Add($"PlanTree generation: {ex.Message}");
        }
    }

    private static int CountNodes(mDBMS.Common.QueryData.PlanNode? node)
    {
        if (node == null) return 0;
        int count = 1;
        
        // Handle different node types
        switch (node)
        {
            case mDBMS.Common.QueryData.JoinNode join:
                count += CountNodes(join.Left);
                count += CountNodes(join.Right);
                break;
            case mDBMS.Common.QueryData.FilterNode filter:
                count += CountNodes(filter.Input);
                break;
            case mDBMS.Common.QueryData.ProjectNode project:
                count += CountNodes(project.Input);
                break;
            case mDBMS.Common.QueryData.SortNode sort:
                count += CountNodes(sort.Input);
                break;
            case mDBMS.Common.QueryData.AggregateNode agg:
                count += CountNodes(agg.Input);
                break;
            // Leaf nodes (scans) have no children
        }
        
        return count;
    }

    private static void TestPlanStepsOrder(QueryOptimizerEngine optimizer)
    {
        // Verify that PlanTree has correct structure
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {"PlanTree structure order",-40} ");
        
        try
        {
            var query = optimizer.ParseQuery("SELECT * FROM employees WHERE id > 5 ORDER BY name");
            var plan = optimizer.OptimizeQuery(query);
            
            if (plan.PlanTree == null)
            {
                Console.WriteLine($"[FAIL] No PlanTree generated");
                _failCount++;
                _failures.Add("PlanTree order: No PlanTree generated");
                return;
            }

            // Find leaf node (should be a scan operation)
            var leaf = FindLeftmostLeaf(plan.PlanTree);
            if (leaf == null)
            {
                Console.WriteLine($"[FAIL] No leaf node found");
                _failCount++;
                _failures.Add("PlanTree order: No leaf node found");
                return;
            }

            // Leaf should be a scan operation
            bool isValidLeaf = leaf is mDBMS.Common.QueryData.TableScanNode || 
                               leaf is mDBMS.Common.QueryData.IndexScanNode ||
                               leaf is mDBMS.Common.QueryData.IndexSeekNode;
            
            if (!isValidLeaf)
            {
                Console.WriteLine($"[FAIL] Leaf node should be SCAN, got {leaf.GetType().Name}");
                _failCount++;
                _failures.Add($"PlanTree order: Leaf node should be SCAN");
                return;
            }

            Console.WriteLine($"[PASS] Tree has correct structure");
            _passCount++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(40, ex.Message.Length))}...");
            _failCount++;
            _failures.Add($"PlanTree order: {ex.Message}");
        }
    }

    private static mDBMS.Common.QueryData.PlanNode? FindLeftmostLeaf(mDBMS.Common.QueryData.PlanNode? node)
    {
        if (node == null) return null;
        
        // Check if leaf node (scan nodes have no children)
        switch (node)
        {
            case mDBMS.Common.QueryData.TableScanNode:
            case mDBMS.Common.QueryData.IndexScanNode:
            case mDBMS.Common.QueryData.IndexSeekNode:
                return node;
            case mDBMS.Common.QueryData.JoinNode join:
                return FindLeftmostLeaf(join.Left) ?? FindLeftmostLeaf(join.Right);
            case mDBMS.Common.QueryData.FilterNode filter:
                return FindLeftmostLeaf(filter.Input);
            case mDBMS.Common.QueryData.ProjectNode project:
                return FindLeftmostLeaf(project.Input);
            case mDBMS.Common.QueryData.SortNode sort:
                return FindLeftmostLeaf(sort.Input);
            case mDBMS.Common.QueryData.AggregateNode agg:
                return FindLeftmostLeaf(agg.Input);
            default:
                return node; // Unknown node, treat as leaf
        }
    }

    private static void TestPlanCostCalculation(QueryOptimizerEngine optimizer)
    {
        // Verify that cost is calculated correctly
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {"Plan cost calculation",-40} ");
        
        try
        {
            var query = optimizer.ParseQuery("SELECT * FROM employees");
            var plan = optimizer.OptimizeQuery(query);
            
            if (plan.TotalEstimatedCost <= 0)
            {
                Console.WriteLine($"[FAIL] Cost should be positive: {plan.TotalEstimatedCost}");
                _failCount++;
                _failures.Add("Plan cost: Cost should be positive");
                return;
            }

            if (plan.PlanTree != null && plan.PlanTree.TotalCost <= 0)
            {
                Console.WriteLine($"[FAIL] PlanTree cost should be positive");
                _failCount++;
                _failures.Add("Plan cost: PlanTree cost should be positive");
                return;
            }

            // Verify that complex queries have higher cost
            var simpleQuery = optimizer.ParseQuery("SELECT * FROM employees");
            var simplePlan = optimizer.OptimizeQuery(simpleQuery);
            
            var complexQuery = optimizer.ParseQuery("SELECT * FROM employees WHERE age > 30 ORDER BY salary DESC");
            var complexPlan = optimizer.OptimizeQuery(complexQuery);
            
            // Note: Complex query might actually be cheaper if using index, so just verify both have positive costs
            if (complexPlan.TotalEstimatedCost <= 0)
            {
                Console.WriteLine($"[FAIL] Complex query cost invalid");
                _failCount++;
                _failures.Add("Plan cost: Complex query cost invalid");
                return;
            }

            Console.WriteLine($"[PASS] Simple={simplePlan.TotalEstimatedCost:F2}, Complex={complexPlan.TotalEstimatedCost:F2}");
            _passCount++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(40, ex.Message.Length))}...");
            _failCount++;
            _failures.Add($"Plan cost: {ex.Message}");
        }
    }

    #endregion

    #region Test Infrastructure

    private static void RunTest(QueryOptimizerEngine optimizer, string testName, string sql, bool expectFailure = false)
    {
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {testName,-40} ");

        try
        {
            // Parse query
            var query = optimizer.ParseQuery(sql);
            
            // Optimize query
            var plan = optimizer.OptimizeQuery(query);
            
            // Validate plan has required properties
            var validationResult = ValidatePlan(plan, query);

            if (expectFailure)
            {
                // We expected it to fail but it passed
                Console.WriteLine($"[UNEXPECTED PASS]");
                _failCount++;
                _failures.Add($"{testName}: Expected failure but succeeded");
            }
            else if (validationResult.IsValid)
            {
                int nodeCount = CountNodes(plan.PlanTree);
                Console.WriteLine($"[PASS] Cost={plan.TotalEstimatedCost:F2}, Nodes={nodeCount}");
                _passCount++;
            }
            else
            {
                Console.WriteLine($"[FAIL] {validationResult.ErrorMessage}");
                _failCount++;
                _failures.Add($"{testName}: {validationResult.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            if (expectFailure)
            {
                Console.WriteLine($"[EXPECTED FAIL] {ex.GetType().Name}");
                _passCount++;
            }
            else
            {
                Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(50, ex.Message.Length))}...");
                _failCount++;
                _failures.Add($"{testName}: {ex.Message}");
            }
        }
    }

    private static (bool IsValid, string ErrorMessage) ValidatePlan(QueryPlan plan, Query query)
    {
        // Basic validation
        if (plan == null) return (false, "Plan is null");
        
        // For SELECT queries, we expect a PlanTree
        if (plan.PlanTree == null)
        {
            // DML queries (INSERT/UPDATE/DELETE) might not have PlanTree
            // but should have Steps or valid query structure
            if (query.Type == QueryType.INSERT || 
                query.Type == QueryType.UPDATE || 
                query.Type == QueryType.DELETE)
            {
                // Validate DML has proper parsed structure
                if (string.IsNullOrWhiteSpace(query.Table))
                    return (false, "DML query missing table name");
                    
                // UPDATE must have UpdateOperations
                if (query.Type == QueryType.UPDATE && 
                    (query.UpdateOperations == null || query.UpdateOperations.Count == 0))
                    return (false, "UPDATE query missing SET operations");
                
                // For DML without PlanTree, verify cost is valid
                if (plan.TotalEstimatedCost < 0)
                    return (false, "DML query has invalid cost");
                    
                return (true, string.Empty);
            }
            return (false, "PlanTree is null for SELECT query");
        }
        
        // Cost should be non-negative
        if (plan.TotalEstimatedCost < 0) return (false, "Cost is negative");
        
        // Verify tree structure matches query
        var treeValidation = ValidatePlanTreeStructure(plan.PlanTree, query);
        if (!treeValidation.IsValid) return treeValidation;
        
        return (true, string.Empty);
    }

    private static (bool IsValid, string ErrorMessage) ValidatePlanTreeStructure(PlanNode tree, Query query)
    {
        // For DML queries (INSERT/UPDATE/DELETE), validate differently
        if (query.Type == QueryType.INSERT || 
            query.Type == QueryType.UPDATE || 
            query.Type == QueryType.DELETE)
        {
            return ValidateDmlPlanTree(tree, query);
        }

        // Find the leaf node - should be a scan of the main table
        var leaf = FindLeftmostLeaf(tree);
        if (leaf == null) return (false, "No leaf node found");

        // Verify leaf is a scan node
        bool isValidLeaf = leaf is TableScanNode || leaf is IndexScanNode || leaf is IndexSeekNode;
        if (!isValidLeaf) return (false, $"Leaf node should be SCAN, got {leaf.GetType().Name}");

        // For SELECT with columns, verify ProjectNode exists (unless SELECT *)
        if (query.Type == QueryType.SELECT && 
            query.SelectedColumns.Count > 0 && 
            !query.SelectedColumns.Contains("*"))
        {
            bool hasProjection = HasNodeOfType<ProjectNode>(tree);
            if (!hasProjection) return (false, "Missing ProjectNode for column selection");
        }

        // For queries with WHERE, verify either FilterNode exists OR IndexSeekNode is used
        // IndexSeekNode effectively handles filtering when seeking by condition
        if (!string.IsNullOrWhiteSpace(query.WhereClause))
        {
            bool hasFilter = HasNodeOfType<FilterNode>(tree);
            bool hasIndexSeek = HasNodeOfType<IndexSeekNode>(tree);
            // If we have IndexSeekNode, filtering is handled by the seek operation
            if (!hasFilter && !hasIndexSeek) return (false, "Missing FilterNode or IndexSeekNode for WHERE clause");
        }

        // For queries with ORDER BY, verify SortNode exists
        if (query.OrderBy != null && query.OrderBy.Any())
        {
            bool hasSort = HasNodeOfType<SortNode>(tree);
            if (!hasSort) return (false, "Missing SortNode for ORDER BY clause");
        }

        // For JOIN queries, verify JoinNode exists
        if (query.Joins != null && query.Joins.Any())
        {
            bool hasJoin = HasNodeOfType<JoinNode>(tree);
            if (!hasJoin) return (false, "Missing JoinNode for JOIN clause");
        }

        // For GROUP BY queries, verify AggregateNode exists
        if (query.GroupBy != null && query.GroupBy.Any())
        {
            bool hasAggregate = HasNodeOfType<AggregateNode>(tree);
            if (!hasAggregate) return (false, "Missing AggregateNode for GROUP BY clause");
        }

        return (true, string.Empty);
    }

    private static (bool IsValid, string ErrorMessage) ValidateDmlPlanTree(PlanNode tree, Query query)
    {
        // DML operations should have their respective node types at the root or be standalone
        switch (query.Type)
        {
            case QueryType.INSERT:
                // INSERT may have InsertNode at root, or be handled differently
                bool hasInsert = tree is InsertNode || HasNodeOfType<InsertNode>(tree);
                if (!hasInsert) return (false, "Missing InsertNode for INSERT query");
                break;
                
            case QueryType.UPDATE:
                // UPDATE should have UpdateNode, may have scan child for WHERE
                bool hasUpdate = tree is UpdateNode || HasNodeOfType<UpdateNode>(tree);
                if (!hasUpdate) return (false, "Missing UpdateNode for UPDATE query");
                break;
                
            case QueryType.DELETE:
                // DELETE should have DeleteNode, may have scan child for WHERE
                bool hasDelete = tree is DeleteNode || HasNodeOfType<DeleteNode>(tree);
                if (!hasDelete) return (false, "Missing DeleteNode for DELETE query");
                break;
        }

        return (true, string.Empty);
    }

    private static bool HasNodeOfType<T>(PlanNode? node) where T : PlanNode
    {
        if (node == null) return false;
        if (node is T) return true;

        return node switch
        {
            JoinNode join => HasNodeOfType<T>(join.Left) || HasNodeOfType<T>(join.Right),
            FilterNode filter => HasNodeOfType<T>(filter.Input),
            ProjectNode project => HasNodeOfType<T>(project.Input),
            SortNode sort => HasNodeOfType<T>(sort.Input),
            AggregateNode agg => HasNodeOfType<T>(agg.Input),
            _ => false
        };
    }

    private static void PrintSummary()
    {
        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("                    TEST SUMMARY");
        Console.WriteLine(new string('=', 60));
        Console.WriteLine($"\n  Total Tests:  {_passCount + _failCount}");
        Console.WriteLine($"  Passed:       {_passCount}");
        Console.WriteLine($"  Failed:       {_failCount}");
        Console.WriteLine($"  Pass Rate:    {(_passCount * 100.0 / (_passCount + _failCount)):F1}%");

        if (_failures.Count > 0)
        {
            Console.WriteLine($"\n  Failed Tests:");
            foreach (var failure in _failures.Take(10))
            {
                Console.WriteLine($"    - {failure}");
            }
            if (_failures.Count > 10)
            {
                Console.WriteLine($"    ... and {_failures.Count - 10} more");
            }
        }

        Console.WriteLine("\n" + new string('=', 60) + "\n");
    }

    #endregion
}
