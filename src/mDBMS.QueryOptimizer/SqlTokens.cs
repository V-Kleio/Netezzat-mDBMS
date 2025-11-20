namespace mDBMS.QueryOptimizer;

/// <summary>
/// Jenis token yang dihasilkan oleh lexer SQL sederhana.
/// </summary>
internal enum SqlTokenType
{
    // Keywords
    SELECT, FROM, WHERE, JOIN, INNER, LEFT, RIGHT, FULL, ON,
    GROUP, BY, ORDER, ASC, DESC, AND, OR,

    // Symbols
    COMMA, DOT, STAR, OPEN_PAREN, CLOSE_PAREN,

    // Operators
    EQUAL, LT, GT, LTE, GTE, NEQ,

    // Literals/Identifiers
    IDENTIFIER, STRING, NUMBER,

    // Special
    EOF
}

/// <summary>
/// Representasi token tunggal.
/// </summary>
internal readonly struct SqlToken
{
    public SqlTokenType Type { get; }
    public string Lexeme { get; }

    public SqlToken(SqlTokenType type, string lexeme)
    {
        Type = type;
        Lexeme = lexeme;
    }

    public override string ToString() => $"{Type}:{Lexeme}";
}

/// <summary>
/// Lexer/Tokenizer untuk subset SQL yang diperlukan optimizer.
/// Mendukung SELECT sederhana: SELECT col1, col2 FROM table [JOIN ... ON ...] [WHERE ...] [GROUP BY ...] [ORDER BY ...].
/// </summary>
internal sealed class SqlLexer
{
    private readonly string _src;
    private int _pos;

    public SqlLexer(string src)
    {
        _src = src ?? string.Empty;
        _pos = 0;
    }

    public List<SqlToken> Tokenize()
    {
        var tokens = new List<SqlToken>();
        SqlToken t;
        do
        {
            t = NextToken();
            tokens.Add(t);
        } while (t.Type != SqlTokenType.EOF);
        return tokens;
    }

    private SqlToken NextToken()
    {
        SkipWhitespace();
        if (IsEof()) return new SqlToken(SqlTokenType.EOF, string.Empty);

        char c = _src[_pos];

        // tanda baca
        switch (c)
        {
            case ',': _pos++; return new SqlToken(SqlTokenType.COMMA, ",");
            case '.': _pos++; return new SqlToken(SqlTokenType.DOT, ".");
            case '*': _pos++; return new SqlToken(SqlTokenType.STAR, "*");
            case '(': _pos++; return new SqlToken(SqlTokenType.OPEN_PAREN, "(");
            case ')': _pos++; return new SqlToken(SqlTokenType.CLOSE_PAREN, ")");
            case '=': _pos++; return new SqlToken(SqlTokenType.EQUAL, "=");
            case '<':
                _pos++;
                if (!IsEof() && _src[_pos] == '=') { _pos++; return new SqlToken(SqlTokenType.LTE, "<="); }
                if (!IsEof() && _src[_pos] == '>') { _pos++; return new SqlToken(SqlTokenType.NEQ, "<>"); }
                return new SqlToken(SqlTokenType.LT, "<");
            case '>':
                _pos++;
                if (!IsEof() && _src[_pos] == '=') { _pos++; return new SqlToken(SqlTokenType.GTE, ">="); }
                return new SqlToken(SqlTokenType.GT, ">");
            case '\'':
                return ReadString();
        }

        if (char.IsDigit(c)) return ReadNumber();
        if (IsIdentStart(c)) return ReadIdentifierOrKeyword();

        // Skip karakter tak dikenal
        _pos++;
        return NextToken();
    }

    private void SkipWhitespace()
    {
        while (!IsEof())
        {
            var ch = _src[_pos];
            if (char.IsWhiteSpace(ch)) { _pos++; continue; }
            // Skip komentar baris (dari string awal --)
            if (ch == '-' && _pos + 1 < _src.Length && _src[_pos + 1] == '-')
            {
                _pos += 2;
                while (!IsEof() && _src[_pos] != '\n' && _src[_pos] != '\r') _pos++;
                continue;
            }
            break;
        }
    }

    private bool IsEof() => _pos >= _src.Length;

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';
    private static bool IsIdentPart(char c) => char.IsLetterOrDigit(c) || c == '_';

    private SqlToken ReadString()
    {
        // Asumsi: karakter current adalah backslash
        int start = ++_pos;
        while (!IsEof())
        {
            if (_src[_pos] == '\'' )
            {
                var literal = _src[start.._pos];
                _pos++;
                return new SqlToken(SqlTokenType.STRING, literal);
            }
            _pos++;
        }
        // String yang tidak terminated, ambil sampai akhir
        return new SqlToken(SqlTokenType.STRING, _src[start..]);
    }

    private SqlToken ReadNumber()
    {
        int start = _pos;
        while (!IsEof() && (char.IsDigit(_src[_pos]) || _src[_pos] == '.')) _pos++;
        var num = _src[start.._pos];
        return new SqlToken(SqlTokenType.NUMBER, num);
    }

    private SqlToken ReadIdentifierOrKeyword()
    {
        int start = _pos;
        while (!IsEof() && IsIdentPart(_src[_pos])) _pos++;
        var ident = _src[start.._pos];
        var upper = ident.ToUpperInvariant();

        return upper switch
        {
            "SELECT" => new SqlToken(SqlTokenType.SELECT, ident),
            "FROM"   => new SqlToken(SqlTokenType.FROM, ident),
            "WHERE"  => new SqlToken(SqlTokenType.WHERE, ident),
            "JOIN"   => new SqlToken(SqlTokenType.JOIN, ident),
            "INNER"  => new SqlToken(SqlTokenType.INNER, ident),
            "LEFT"   => new SqlToken(SqlTokenType.LEFT, ident),
            "RIGHT"  => new SqlToken(SqlTokenType.RIGHT, ident),
            "FULL"   => new SqlToken(SqlTokenType.FULL, ident),
            "ON"     => new SqlToken(SqlTokenType.ON, ident),
            "GROUP"  => new SqlToken(SqlTokenType.GROUP, ident),
            "BY"     => new SqlToken(SqlTokenType.BY, ident),
            "ORDER"  => new SqlToken(SqlTokenType.ORDER, ident),
            "ASC"    => new SqlToken(SqlTokenType.ASC, ident),
            "DESC"   => new SqlToken(SqlTokenType.DESC, ident),
            "AND"    => new SqlToken(SqlTokenType.AND, ident),
            "OR"     => new SqlToken(SqlTokenType.OR, ident),
            _         => new SqlToken(SqlTokenType.IDENTIFIER, ident)
        };
    }
}
