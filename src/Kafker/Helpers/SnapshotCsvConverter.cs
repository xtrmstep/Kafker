using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JsonFlatten;
using Kafker.Configurations;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public class SnapshotCsvConverter
    {
        private readonly IDictionary<string, string> _csvMapping;

        public SnapshotCsvConverter(KafkaTopicConfiguration topicConfiguration)
        {
            _csvMapping = topicConfiguration != null ? topicConfiguration.Mapping : new Dictionary<string, string>();
        }

        private void AddTableRow(DataTable dataTable, JObject json)
        {            
            var dic = json.Flatten();
            var columns = new List<string>(dic.Keys);
            var row = dataTable.NewRow();
            // add missing columns
            foreach (var column in columns)
            {
                var mapNotNullAndKeyExists = _csvMapping.Any() && _csvMapping.ContainsKey(column);
                var renamedColumnName = column;
                if (mapNotNullAndKeyExists)
                {
                    renamedColumnName = _csvMapping[column];
                }

                if (!dataTable.Columns.Contains(renamedColumnName))
                {
                    var shouldAddColumn = !_csvMapping.Any() || mapNotNullAndKeyExists;
                    if (!shouldAddColumn) continue;

                    var dataColumn = new DataColumn(renamedColumnName, typeof(object));
                    dataTable.Columns.Add(dataColumn);
                }
                row[renamedColumnName] = dic[column];
            }
            dataTable.Rows.Add(row);
        }

        private async Task SaveTableToFileAsync(DataTable dataTable, string destinationCsvFile)
        {
            await Task.Yield();
            CSVLibraryAK.Core.CSVLibraryAK.Export(destinationCsvFile, dataTable);
        }

        private async Task<List<JObject>> LoadFromSnapshotAsync(string sourceFile)
        {
            var lines = await File.ReadAllLinesAsync(sourceFile);
            var list = lines
                .Select(line => line.Split("|")[1])
                .Select(record => JsonConvert.DeserializeObject<JObject>(record, 
                    new JsonSerializerSettings {DateParseHandling = DateParseHandling.None}))
                .ToList();

            return list;
        }

        private DataTable LoadJsonsToTable(IList<JObject> list)
        {
            var tbl = new DataTable();
            foreach (var item in list)
            {
                AddTableRow(tbl, item);
            }
            return tbl;
        }

        public async Task ConvertAndSaveAsync(string fileName)
        {
            var destinationFile = fileName.Replace(".dat", ".csv");
            var listOfJson = await LoadFromSnapshotAsync(fileName);
            var tbl = LoadJsonsToTable(listOfJson);
            await SaveTableToFileAsync(tbl, destinationFile);
        }
    }
}