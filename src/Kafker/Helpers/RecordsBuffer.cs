using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using JsonFlatten;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public class RecordsBuffer
    {
        private readonly IConsole _console;
        private DataTable _tbl = new DataTable();
        private List<KeyValuePair<Timestamp, string>> _buffer = new List<KeyValuePair<Timestamp, string>>();

        public RecordsBuffer(IConsole console)
        {
            _console = console;
        }
        
        public void Add(Timestamp messageTimestamp, string json)
        {
            var pair = new KeyValuePair<Timestamp,string>(messageTimestamp, json);
            _buffer.Add(pair);
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
            //CSVLibraryAK.Core.CSVLibraryAK.Export(sourceFile.FullName, _tbl);

            await using var fs = File.CreateText(destinationCsvFile.FullName);
            float total = _buffer.Count;
            float idx = 0;
            foreach (var pair in _buffer)
            {
                await fs.WriteLineAsync($"\"{pair.Key.UnixTimestampMs}\"|\"{pair.Value}\"");
                await _console.Out.WriteAsync($"\rstored {++idx / total * 100:f2}% [{idx:f0}/{total:f0}]");
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
            //_tbl = CSVLibraryAK.Core.CSVLibraryAK.Import(sourceFile, true);
            var lines = await File.ReadAllLinesAsync(sourceFile);
            var idx = 0;
            foreach (var line in lines)
            {
                var pair = line.Split("|");
                var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                var record = pair[1].Substring(1, pair[1].Length - 2);
                var item = new KeyValuePair<Timestamp, string>(new Timestamp(long.Parse(timestamp),TimestampType.CreateTime), record);
                _buffer.Add(item);
                
                await _console.Out.WriteAsync($"\rloaded {++idx}...");
            }
        }

        public IEnumerable<string> GetRecords()
        {
            return _buffer.Select(p => p.Value).ToArray();
        }
    }
}