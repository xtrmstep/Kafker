using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using JsonFlatten;
using Kafker.Configurations;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public class RecordsBuffer
    {
        private DataTable _tbl = new DataTable();
        private Dictionary<Timestamp, string> _buffer = new Dictionary<Timestamp, string>();

        public void Add(Timestamp messageTimestamp, string json)
        {
            _buffer.Add(messageTimestamp, json);
        }

        public void Convert(JObject json)
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

        public async Task SaveToFileAsync(FileInfo destinationCsvFile)
        {
            await Task.Yield();
            //CSVLibraryAK.CSVLibraryAK.Export(sourceFile.FullName, _tbl);

            await using var fs = File.CreateText(destinationCsvFile.FullName);
            foreach (var pair in _buffer)
            {
                await fs.WriteLineAsync($"\"{pair.Key.UnixTimestampMs}\"|\"{pair.Value}\"");
            }

            await fs.FlushAsync();
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
        
        public async Task LoadFromFileAsync(string sourceFile)
        {
            await Task.Yield();
            //_tbl = CSVLibraryAK.CSVLibraryAK.Import(sourceFile, true);
            var lines = await File.ReadAllLinesAsync(sourceFile);
            foreach (var line in lines)
            {
                var pair = line.Split("|");
                var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                var record = pair[1].Substring(1, pair[1].Length - 2);
                _buffer.Add(new Timestamp(long.Parse(timestamp),TimestampType.CreateTime), record);
            }
        }

        public IEnumerable<string> GetRecords()
        {
            return _buffer.Values.ToArray();
        }
    }
}