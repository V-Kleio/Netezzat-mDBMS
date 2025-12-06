using System.Text;
using System.Text.RegularExpressions;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer;

/// <summary>
/// Parser sederhana untuk subset SQL SELECT yang digunakan oleh optimizer.
/// Memproduksi objek <see cref="Query"/> yang berisi struktur dasar kueri.
/// </summary>
internal sealed class SqlParser
{
    private readonly List<SqlToken> _tokens;
    private int _pos;

    public SqlParser(List<SqlToken> tokens)
    {
        _tokens = tokens ?? new();
        _pos = 0;
    }

    private SqlToken Peek(int ahead = 0)
    {
        var idx = Math.Min(_pos + ahead, _tokens.Count - 1);
        return _tokens[idx];
    }

    private SqlToken Consume() => _tokens[_pos++];
    private bool Match(SqlTokenType t) { if (Peek().Type == t) { _pos++; return true; } return false; }

    /// <summary>
    /// Entry Point untuk parsing SQL.
    /// </summary>
    public Query Parser()
    {
        var first = Peek();
        return first.Type switch
        {
            SqlTokenType.SELECT => ParseSelect(),
            SqlTokenType.UPDATE => ParseUpdate(),
            SqlTokenType.INSERT => ParseInsert(),
            SqlTokenType.DELETE => ParseDelete(),
            _ => throw new InvalidOperationException($"Unsupported query type: {first.Type}")
        };
    }

    private List<string> ParseTableList()
    {
        var tables = new List<string>
        {
            ParseIdentifier()
        };

        while (Match(SqlTokenType.COMMA))
        {
            tables.Add(ParseIdentifier());
        }

        return tables;
    }

    /// <summary>
    /// Parse SELECT ... FROM ... [JOIN ... ON ...]* [WHERE ...] [GROUP BY ...] [ORDER BY ...]
    /// </summary>
    public Query ParseSelect()
    {
        var q = new Query { Type = QueryType.SELECT };

        Expect(SqlTokenType.SELECT);
        q.SelectedColumns = ParseSelectList();
        Expect(SqlTokenType.FROM);
        var tables = ParseTableList();

        Console.WriteLine($"[SqlParser] Parsed {tables.Count} tables from FROM clause: {string.Join(", ", tables)}");

        if (tables.Count == 1)
        {
            q.Table = tables[0];
            q.FromTables = null;
        }
        else
        {
            q.Table = tables[0];
            q.FromTables = tables;
        }

        // JOINs
        var joins = new List<JoinOperation>();
        while (IsJoinStart())
        {
            string leftTable = tables.Count > 1 ? string.Join(",", tables) : q.Table;
            joins.Add(ParseJoin(leftTable));
        }
        if (joins.Count > 0) q.Joins = joins;

        // WHERE
        if (Match(SqlTokenType.WHERE))
        {
            q.WhereClause = ReadUntilKeywords(SqlTokenType.GROUP, SqlTokenType.ORDER, SqlTokenType.EOF).Trim();
        }

        // GROUP BY
        if (Match(SqlTokenType.GROUP))
        {
            Expect(SqlTokenType.BY);
            q.GroupBy = ParseIdentifierList();
        }

        // ORDER BY
        if (Match(SqlTokenType.ORDER))
        {
            Expect(SqlTokenType.BY);
            q.OrderBy = ParseOrderByList();
        }

        return q;
    }

    /// <summary>
    /// Parse UPDATE ... SET ... [WHERE ...]
    /// </summary>
    public Query ParseUpdate()
    {
        var q = new Query { Type = QueryType.UPDATE };
        Expect(SqlTokenType.UPDATE);
        q.Table = ParseIdentifier();
        Expect(SqlTokenType.SET);
        q.UpdateOperations = ParseSetList();
        if (Match(SqlTokenType.WHERE))
        {
            q.WhereClause = ReadUntilKeywords(SqlTokenType.EOF).Trim();
        }
        if (Peek().Type != SqlTokenType.EOF)
        {
            throw new InvalidOperationException($"Unexpected token after end of UPDATE: {Peek().Lexeme}");
        }
        return q;
    }
    /// <summary>
    /// Parse INSERT INTO ... VALUES ...
    /// Parse INSERT INTO ... SELECT ... FROM ...
    /// </summary>
    public Query ParseInsert()
    {
        var q = new Query { Type = QueryType.INSERT };

        Expect(SqlTokenType.INSERT);
        Expect(SqlTokenType.INTO);
        q.Table = ParseIdentifier();

        // Optional: explicit column list
        if (Match(SqlTokenType.OPEN_PAREN))
        {
            // Check if this is column list or VALUES
            // Heuristic: jika setelah '(' ada identifier diikuti ',' atau ')', ini column list
            int savedPos = _pos;

            try
            {
                var columns = ParseColumnList();

                if (Match(SqlTokenType.CLOSE_PAREN))
                {
                    q.InsertColumns = columns;
                }
                else
                {
                    _pos = savedPos - 1;
                }
            }
            catch
            {
                _pos = savedPos - 1;
            }
            // if (Peek().Type == SqlTokenType.IDENTIFIER && !IsValueLiteral(Peek(1)))
            // {
            //     q.InsertColumns = ParseColumnList();
            //     Expect(SqlTokenType.CLOSE_PAREN);
            // }
            // else
            // {
            //     // Ini adalah VALUES tanpa explicit columns
            //     // Backtrack: kembalikan '(' untuk parsing VALUES
            //     _pos--;
            // }
        }

        // VALUES atau SELECT
        if (Match(SqlTokenType.VALUES))
        {
            q.InsertValues = ParseValuesList();

            if (q.InsertColumns != null && q.InsertValues != null && q.InsertValues.Count > 0)
            {
                int expectedCount = q.InsertColumns.Count;
                int actualCount = q.InsertValues[0].Count;

                if (expectedCount != actualCount)
                {
                    throw new InvalidOperationException(
                        $"Column count mismatch: {expectedCount} columns specified, but {actualCount} values provided");
                }
            }
        }
        else if (Peek().Type == SqlTokenType.SELECT)
        {
            q.InsertFromQuery = ParseSelect();

            if (q.InsertColumns != null && q.InsertFromQuery != null)
            {
                int expectedCount = q.InsertColumns.Count;
                int actualCount = q.InsertFromQuery.SelectedColumns.Count;

                if (!q.InsertFromQuery.SelectedColumns.Contains("*") && expectedCount != actualCount)
                {
                    throw new InvalidOperationException(
                        $"Column count mismatch: {expectedCount} columns specified, but SELECT returns {actualCount} columns");
                }
            }
        }
        else
        {
            throw new InvalidOperationException("Expected VALUES or SELECT after INSERT INTO table");
        }

        // Validate EOF
        if (Peek().Type != SqlTokenType.EOF)
        {
            throw new InvalidOperationException($"Unexpected token after INSERT: {Peek().Lexeme}");
        }

        return q;
    }
    /// <summary>
    /// PARSE DELETE FROM ... [WHERE ...]
    /// </summary>
    public Query ParseDelete()
    {
        var q = new Query { Type = QueryType.DELETE };

        Expect(SqlTokenType.DELETE);
        Expect(SqlTokenType.FROM);
        q.Table = ParseIdentifier();

        // Optional WHERE clause
        if (Match(SqlTokenType.WHERE))
        {
            q.WhereClause = ReadUntilKeywords(SqlTokenType.EOF).Trim();
        }

        // Validate EOF
        if (Peek().Type != SqlTokenType.EOF)
        {
            throw new InvalidOperationException($"Unexpected token after DELETE: {Peek().Lexeme}");
        }

        return q;
    }
    /// <summary>
    /// Parse daftar kolom dan nilai untuk klausa SET pada UPDATE.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    private Dictionary<string, string> ParseSetList()
    {
        var updates = new Dictionary<string, string>();
        if (Peek().Type == SqlTokenType.WHERE || Peek().Type == SqlTokenType.EOF)
            throw new InvalidOperationException("SET clause cannot be empty");
        do {
            var col = ParseIdentifierWithOptionalDot();
            if (updates.ContainsKey(col))
            {
                throw new InvalidOperationException($"Duplicate column in SET list: {col}");
            }
            Expect(SqlTokenType.EQUAL);
            var val = ReadValueOrExpression();
            updates[col] = val;
        } while (Match(SqlTokenType.COMMA));
        return updates;
    }

    private string ReadValueOrExpression()
    {
        var sb = new StringBuilder();
        int depth = 0;
        bool hasToken = false;
        int maxIterations = 1000000;
        int iterations = 0;

        while (iterations++ < maxIterations) {
            var t = Peek();
            if (t.Type == SqlTokenType.EOF || (depth == 0 && (t.Type == SqlTokenType.COMMA || t.Type == SqlTokenType.WHERE)))
            {
                break;
            }
            if (t.Type == SqlTokenType.EQUAL)
            {
                throw new InvalidOperationException("Unexpected '=' inside SET expression");
            }
            if (t.Type == SqlTokenType.OPEN_PAREN) depth++;
            if (t.Type == SqlTokenType.CLOSE_PAREN)
            {
                depth--;
                if (depth < 0)
                    {
                        throw new InvalidOperationException("Unbalanced closing parenthesis in SET expression");
                    }
            }
            hasToken = true;
            sb.Append(Consume().Lexeme + " ");
        }

        if (iterations >= maxIterations)
        {
            throw new InvalidOperationException("Expression too complex or infinite loop detected in SET clause");
        }
        if (!hasToken)
        {
            throw new InvalidOperationException("Empty value in SET assignment");
        }
        if (depth != 0)
        {
            throw new InvalidOperationException("Unbalanced parentheses in SET expression");
        }
        return sb.ToString().Trim();
    }

    private void Expect(SqlTokenType type)
    {
        var t = Consume();
        if (t.Type != type)
            throw new InvalidOperationException($"Syntax error: expected {type} but found {t.Type} at position {_pos}");
    }

    private string ParseIdentifier()
    {
        var t = Consume();
        if (t.Type == SqlTokenType.IDENTIFIER || t.Type == SqlTokenType.STRING)
            return t.Lexeme;
        throw new InvalidOperationException($"Identifier expected, got {t.Type}");
    }

    private List<string> ParseIdentifierList()
    {
        var list = new List<string> { ParseIdentifier() };
        while (Match(SqlTokenType.COMMA))
        {
            list.Add(ParseIdentifier());
        }
        return list;
    }

    private List<string> ParseSelectList()
    {
        var cols = new List<string>();
        if (Match(SqlTokenType.STAR)) { cols.Add("*"); return cols; }

        cols.Add(ParseIdentifierWithOptionalDot());
        while (Match(SqlTokenType.COMMA))
        {
            cols.Add(ParseIdentifierWithOptionalDot());
        }
        return cols;
    }

    private string ParseIdentifierWithOptionalDot()
    {
        var id = ParseIdentifier();
        if (Match(SqlTokenType.DOT))
        {
            var right = ParseIdentifier();
            return id + "." + right;
        }
        return id;
    }

    private bool IsJoinStart()
    {
        var t = Peek().Type;
        return t == SqlTokenType.JOIN || t == SqlTokenType.INNER || t == SqlTokenType.LEFT || t == SqlTokenType.RIGHT || t == SqlTokenType.FULL;
    }

    private JoinOperation ParseJoin(string leftTable)
    {
        var jt = JoinType.INNER;
        if (Match(SqlTokenType.INNER)) jt = JoinType.INNER;
        else if (Match(SqlTokenType.LEFT)) jt = JoinType.LEFT;
        else if (Match(SqlTokenType.RIGHT)) jt = JoinType.RIGHT;
        else if (Match(SqlTokenType.FULL)) jt = JoinType.FULL;

        Expect(SqlTokenType.JOIN);
        var rightTable = ParseIdentifier();
        Expect(SqlTokenType.ON);
        var on = ReadUntilKeywords(SqlTokenType.WHERE, SqlTokenType.GROUP, SqlTokenType.ORDER, SqlTokenType.JOIN, SqlTokenType.EOF).Trim();

        return new JoinOperation
        {
            LeftTable = leftTable,
            RightTable = rightTable,
            OnCondition = on,
            Type = jt
        };
    }

    private string ReadUntilKeywords(params SqlTokenType[] stopTokens)
    {
        var sb = new StringBuilder();
        while (true)
        {
            var t = Peek();
            if (stopTokens.Contains(t.Type)) break;
            if (t.Type == SqlTokenType.EOF) break;

            var lexeme = Consume().Lexeme;
            sb.Append(lexeme);

            var next = Peek();
            if (next.Type != SqlTokenType.DOT && t.Type != SqlTokenType.DOT &&
                next.Type != SqlTokenType.EOF && !stopTokens.Contains(next.Type))
            {
                sb.Append(' ');
            }
        }
        return sb.ToString();
    }

    private List<OrderByOperation> ParseOrderByList()
    {
        var orderList = new List<OrderByOperation>();

        do
        {
            var col = ParseIdentifierWithOptionalDot();
            var isAsc = true;

            if (Match(SqlTokenType.ASC))
            {
                isAsc = true;
            }
            else if (Match(SqlTokenType.DESC))
            {
                isAsc = false;
            }

            orderList.Add(new OrderByOperation
            {
                Column = col,
                IsAscending = isAsc
            });

        } while (Match(SqlTokenType.COMMA));

        return orderList;
    }

    private List<string> ParseColumnList()
    {
        var cols = new List<string>();

        do
        {
            cols.Add(ParseIdentifier());
        } while (Match(SqlTokenType.COMMA));

        return cols;
    }

    private List<List<string>> ParseValuesList()
    {
        var allRows = new List<List<string>>();

        do
        {
            Expect(SqlTokenType.OPEN_PAREN);
            var oneRow = ParseSingleValueRow();
            if (oneRow.Count == 0)
            {
                throw new InvalidOperationException("VALUES list cannot be empty");
            }
            Expect(SqlTokenType.CLOSE_PAREN);
            allRows.Add(oneRow);

        } while (Match(SqlTokenType.COMMA));

        // Validate all rows have same column count
        if (allRows.Count > 1)
        {
            int expectedCount = allRows[0].Count;
            for (int i = 1; i < allRows.Count; i++)
            {
                if (allRows[i].Count != expectedCount)
                {
                    throw new InvalidOperationException(
                        $"Row {i + 1} has {allRows[i].Count} values, expected {expectedCount}");
                }
            }
        }

        return allRows;
    }

    private List<string> ParseSingleValueRow()
    {
        var values = new List<string>();

        do
        {
            values.Add(ParseValue());
        } while (Match(SqlTokenType.COMMA));

        return values;
    }

    private string ParseValue()
    {
        var token = Peek();

        // 1. DEFAULT keyword
        if (Match(SqlTokenType.DEFAULT))
        {
            return "DEFAULT";
        }

        // 2. String literal
        if (token.Type == SqlTokenType.STRING)
        {
            return "'" + Consume().Lexeme + "'";
        }

        // 3. Number literal
        if (token.Type == SqlTokenType.NUMBER)
        {
            return Consume().Lexeme;
        }

        // 4. NULL or identifier (function call, column reference)
        if (token.Type == SqlTokenType.IDENTIFIER)
        {
            var ident = Consume().Lexeme;

            if (ident.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                return "NULL";
            }

            // Check for function call: FUNC(...)
            if (Match(SqlTokenType.OPEN_PAREN))
            {
                var args = ParseFunctionArgs();
                Expect(SqlTokenType.CLOSE_PAREN);
                return $"{ident}({string.Join(", ", args)})";
            }

            return ident;
        }

        // 5. Nested expression with parentheses
        if (Match(SqlTokenType.OPEN_PAREN))
        {
            var expr = ParseValue();
            Expect(SqlTokenType.CLOSE_PAREN);
            return $"({expr})";
        }

        throw new InvalidOperationException($"Unexpected token in VALUES: {token.Type} '{token.Lexeme}'");
    }

    private List<string> ParseFunctionArgs()
    {
        var args = new List<string>();

        // Empty function: FUNC()
        if (Peek().Type == SqlTokenType.CLOSE_PAREN)
        {
            return args;
        }

        do
        {
            args.Add(ParseValue());
        } while (Match(SqlTokenType.COMMA));

        return args;
    }

    private bool IsValueLiteral(SqlToken token)
    {
        return token.Type == SqlTokenType.STRING
            || token.Type == SqlTokenType.NUMBER
            || token.Type == SqlTokenType.DEFAULT
            || token.Lexeme.Equals("NULL", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// Kumpulan helper untuk parsing dan analisis predicate.
/// </summary>
internal static class SqlParserHelpers
{
    private static readonly Regex ColumnRegex = new(
        pattern: @"\b([A-Za-z_][A-Za-z0-9_]*)(?:\.[A-Za-z_][A-Za-z0-9_]*)?\s*(=|<>|!=|<=|>=|<|>)\s*(?:'[\s\S]*?'|[A-Za-z_][A-Za-z0-9_]*|\d+(?:\.\d+)?)",
        options: RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Ekstrak kandidat nama kolom dari klausa WHERE untuk heuristik indeks.
    /// </summary>
    public static IEnumerable<string> ExtractPredicateColumns(string? whereClause)
    {
        if (string.IsNullOrWhiteSpace(whereClause)) yield break;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match m in ColumnRegex.Matches(whereClause))
        {
            if (m.Success && m.Groups.Count > 1)
            {
                var col = m.Groups[1].Value;
                if (seen.Add(col)) yield return col;
            }
        }
    }
}
