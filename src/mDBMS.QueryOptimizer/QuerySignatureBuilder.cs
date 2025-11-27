using System.Text;
using mDBMS.Common.QueryData;

namespace mDBMS.QueryOptimizer;

internal static class QuerySignatureBuilder {

    public static string Build(Query query) {
        var sb = new StringBuilder();
        sb.Append("SELECT|");
        AppendList(sb, query.SelectedColumns.OrderBy(c => c, StringComparer.OrdinalIgnoreCase));

        sb.Append("|FROM|").Append(query.Table.ToLowerInvariant());
        
        sb.Append("|JOINS|");
        if (query.Joins != null) {
            foreach (var join in query.Joins) {
                sb.Append(join.Type).Append(":")
                .Append(join.LeftTable.ToLowerInvariant()).Append("->")
                .Append(join.RightTable.ToLowerInvariant()).Append("|ON|")
                .Append(NormalizeWhitespace(join.OnCondition));
                sb.Append("|");
            }
        }

        sb.Append("|WHERE|");
        sb.Append(NormalizeWhitespace(query.WhereClause));

        sb.Append("|GROUP|");
        AppendList(sb, query.GroupBy ?? Enumerable.Empty<string>());

        sb.Append("|ORDER|");
        if (query.OrderBy != null)
        {
            foreach (var order in query.OrderBy)
            {
                sb.Append(order.Column.ToLowerInvariant())
                  .Append(order.IsAscending ? ":ASC" : ":DESC")
                  .Append(";");
            }
        }
        return sb.ToString();
    }

    private static void AppendList(StringBuilder sb, IEnumerable<string> items) {
        foreach (var item in items) {
            sb.Append(item.ToUpperInvariant()).Append(",");
        }
    }

    private static string NormalizeWhitespace(string? input) {
        if (string.IsNullOrWhiteSpace(input)) {
            return string.Empty;
        }
        var builder = new StringBuilder();
        bool inWhitespace = false;
        foreach (char c in input.Trim()) {
            if (char.IsWhiteSpace(c)) {
                if (!inWhitespace) {
                    builder.Append(' ');
                    inWhitespace = true;
                }
            } else {
                builder.Append(char.ToLowerInvariant(c));
                inWhitespace = false;
            }
        }
        return builder.ToString();
    }
}