namespace Kafker.Csv
{
    public abstract class CsvFileBase
    {
        protected const string CSV_VALUE_SEPARATOR = ",";
        protected const string CSV_VALUE_WRAPPER = "\"";
        protected string[] CSV_VALUE_REPLACEMENTS = 
        {
            "System.Linq.EmptyPartition`1[System.Object]"
        };
    }
}