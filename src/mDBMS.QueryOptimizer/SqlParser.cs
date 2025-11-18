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
    /// Parse SELECT ... FROM ... [JOIN ... ON ...]* [WHERE ...] [GROUP BY ...] [ORDER BY ...]
    /// </summary>
    public Query ParseSelect()
    {
        var q = new Query { Type = QueryType.SELECT };

        Expect(SqlTokenType.SELECT);
        q.SelectedColumns = ParseSelectList();
        Expect(SqlTokenType.FROM);
        q.Table = ParseIdentifier();

        // JOINs
        var joins = new List<JoinOperation>();
        while (IsJoinStart())
        {
            joins.Add(ParseJoin(q.Table));
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
            sb.Append(Consume().Lexeme);
            sb.Append(' ');
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
