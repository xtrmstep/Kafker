using System.Text.RegularExpressions;

namespace Kafker.Helpers
{
    public static class CsvParseHelper
    {
        public static string[] GetValues(string line, string valueSeparator, string valueWrapper)
        {
            // test sample:
            // text",3.0 - \"\s*,\s*(?!\d)?
            // 3.0,"text - (?!\d)?\s*,\s*\"
            // text","text - \"\s*,\s*\"
            var pattern = @"\WRP\s*SEP\s*\WRP|(?!\d)?\s*SEP\s*\WRP|\WRP\s*SEP\s*(?!\d)?"
                .Replace("WRP", valueWrapper)
                .Replace("SEP", valueSeparator);
            var values = Regex.Split(line, pattern);
            
            if (values[0][0] == '"')
            {
                values[0] = values[0].Substring(1, values[0].Length - 1);
            }
            if (values[^1][^1] == '"')
            {
                values[^1] = values[^1].Substring(0, values[^1].Length - 1);
            }

            return values;
        }
    }
}