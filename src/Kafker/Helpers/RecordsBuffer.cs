using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JsonFlatten;
using Kafker.Configurations;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public class RecordsBuffer
    {
        private DataTable _tbl = new DataTable();
        
        public void Add(JObject json)
        {
            var dic = json.Flatten();
            var columns = new List<string>(dic.Keys);
            // add missing columns
            foreach (var column in columns)
            {
                if (_tbl.Columns.Contains(column)) continue;
                
                var dataColumn = new DataColumn(column, typeof(object));
                _tbl.Columns.Add(dataColumn);
            }
            // add values
            var row = _tbl.NewRow();
            foreach (var column in columns)
            {
                row[column] = dic[column];
            }
            _tbl.Rows.Add(row);
        }

        public async Task SaveToFileAsync(FileInfo destinationCsvFile, TopicMappingConfiguration topicMappingConfiguration)
        {
            await Task.Yield();
            CSVLibraryAK.CSVLibraryAK.Export(destinationCsvFile.FullName, _tbl);
        }

        public async Task<JObject[]> GetJsonRecordsAsync(TopicMappingConfiguration topicMappingConfiguration)
        {
            await Task.Yield();
            var result = new List<JObject>();
            foreach (DataRow dataRow in _tbl.Rows)
            {
                var dicRow = new Dictionary<string, object>();
                foreach (DataColumn dataColumn in _tbl.Columns)
                {
                    var val = dataRow[dataColumn];
                    dicRow.Add(dataColumn.ColumnName, val);
                }

                var job = dicRow.Unflatten();
                result.Add(job);
            }

            return result.ToArray();
        }
        
        public async Task LoadFromFileAsync(string destinationCsvFile)
        {
            await Task.Yield();
            _tbl = CSVLibraryAK.CSVLibraryAK.Import(destinationCsvFile, true);
        }
    }
}