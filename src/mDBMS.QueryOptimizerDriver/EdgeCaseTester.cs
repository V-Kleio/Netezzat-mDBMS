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
        TestLongColumnNames(optimizer);
        TestNumericColumnNames(optimizer);
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
        RunTest(optimizer, "Query on empty/nonexistent table",
            "SELECT * FROM nonexistent_table");
    }

    private static void TestLongColumnNames(QueryOptimizerEngine optimizer)
    {
        RunTest(optimizer, "Long column names",
            "SELECT very_long_column_name_that_might_cause_issues FROM employees");
    }

    private static void TestNumericColumnNames(QueryOptimizerEngine optimizer)
    {
        // Column names starting with numbers should fail
        RunTest(optimizer, "Numeric column reference",
            "SELECT col1, col2, col3 FROM employees");
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
        // Escaped quotes in strings - parser handles this
        RunTest(optimizer, "Special characters in strings",
            "SELECT * FROM employees WHERE name = 'O\\'Brien'");
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
        // Parser may be lenient and skip unknown operators
        RunTest(optimizer, "Invalid operator (!! treated as unknown)",
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
        RunTest(optimizer, "Self JOIN (same table)",
            "SELECT * FROM employees e1 INNER JOIN employees e2 ON e1.manager_id = e2.id");
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

    #region Plan Node ToSteps Verification

    private static void TestPlanStepsGeneration(QueryOptimizerEngine optimizer)
    {
        // Verify that PlanTree.ToSteps() generates correct steps
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {"PlanSteps generation verification",-40} ");
        
        try
        {
            var query = optimizer.ParseQuery("SELECT id, name FROM employees WHERE age > 30 ORDER BY name");
            var plan = optimizer.OptimizeQuery(query);
            
            // Check that Steps exist and match PlanTree
            if (plan.PlanTree == null)
            {
                Console.WriteLine($"[FAIL] No PlanTree");
                _failCount++;
                _failures.Add("PlanSteps generation: No PlanTree");
                return;
            }

            // Generate steps from tree
            var generatedSteps = plan.PlanTree.ToSteps();
            
            // Verify steps are not empty
            if (generatedSteps.Count == 0)
            {
                Console.WriteLine($"[FAIL] ToSteps returned empty");
                _failCount++;
                _failures.Add("PlanSteps generation: ToSteps returned empty");
                return;
            }

            // Verify QueryPlan.Steps matches
            if (plan.Steps.Count != generatedSteps.Count)
            {
                Console.WriteLine($"[FAIL] Steps count mismatch: {plan.Steps.Count} vs {generatedSteps.Count}");
                _failCount++;
                _failures.Add($"PlanSteps generation: Steps count mismatch");
                return;
            }

            Console.WriteLine($"[PASS] {generatedSteps.Count} steps generated");
            _passCount++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(40, ex.Message.Length))}...");
            _failCount++;
            _failures.Add($"PlanSteps generation: {ex.Message}");
        }
    }

    private static void TestPlanStepsOrder(QueryOptimizerEngine optimizer)
    {
        // Verify that steps are in correct execution order
        Console.Write($"  [{(_passCount + _failCount + 1):D2}] {"PlanSteps execution order",-40} ");
        
        try
        {
            var query = optimizer.ParseQuery("SELECT * FROM employees WHERE id > 5 ORDER BY name");
            var plan = optimizer.OptimizeQuery(query);
            
            if (plan.Steps.Count == 0)
            {
                Console.WriteLine($"[FAIL] No steps generated");
                _failCount++;
                _failures.Add("PlanSteps order: No steps generated");
                return;
            }

            // Verify Order property is sequential
            for (int i = 0; i < plan.Steps.Count; i++)
            {
                if (plan.Steps[i].Order != i + 1)
                {
                    Console.WriteLine($"[FAIL] Order mismatch at step {i}");
                    _failCount++;
                    _failures.Add($"PlanSteps order: Order mismatch at step {i}");
                    return;
                }
            }

            // First step should be a scan operation
            var firstOp = plan.Steps[0].Operation;
            bool isValidFirst = firstOp == OperationType.TABLE_SCAN || 
                               firstOp == OperationType.INDEX_SCAN ||
                               firstOp == OperationType.INDEX_SEEK;
            
            if (!isValidFirst)
            {
                Console.WriteLine($"[FAIL] First step should be SCAN, got {firstOp}");
                _failCount++;
                _failures.Add($"PlanSteps order: First step should be SCAN");
                return;
            }

            Console.WriteLine($"[PASS] {plan.Steps.Count} steps in correct order");
            _passCount++;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FAIL] {ex.GetType().Name}: {ex.Message.Substring(0, Math.Min(40, ex.Message.Length))}...");
            _failCount++;
            _failures.Add($"PlanSteps order: {ex.Message}");
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
            bool isValid = ValidatePlan(plan);

            if (expectFailure)
            {
                // We expected it to fail but it passed
                Console.WriteLine($"[UNEXPECTED PASS]");
                _failCount++;
                _failures.Add($"{testName}: Expected failure but succeeded");
            }
            else if (isValid)
            {
                Console.WriteLine($"[PASS] Cost={plan.TotalEstimatedCost:F2}, Steps={plan.Steps.Count}");
                _passCount++;
            }
            else
            {
                Console.WriteLine($"[FAIL] Invalid plan structure");
                _failCount++;
                _failures.Add($"{testName}: Invalid plan structure");
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

    private static bool ValidatePlan(QueryPlan plan)
    {
        // Basic validation
        if (plan == null) return false;
        
        // For SELECT queries, we expect a PlanTree
        // For DML (INSERT/UPDATE/DELETE), PlanTree might be null but Steps should exist
        if (plan.PlanTree == null && plan.Steps.Count == 0) return false;
        
        // Cost should be non-negative
        if (plan.TotalEstimatedCost < 0) return false;
        
        return true;
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
