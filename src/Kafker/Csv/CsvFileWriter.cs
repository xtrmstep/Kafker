using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JsonFlatten;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json.Linq;

namespace Kafker.Csv
{
    public class CsvFileWriter : CsvFileBase, ICsvFileWriter
    {
        private string _csvHeader;
        private bool _isCsvHeaderWrittenToFile;
        private IDictionary<string, string> _jsonToCsvMapping;
        private StreamWriter _streamWriter;

        public CsvFileWriter(FileSystemInfo fileInfo, TopicMappingConfiguration mapping, IConsole console)
        {
            InitCsvFile(fileInfo, console);
            InitMapping(mapping);
        }

        public async Task WriteAsync(CancellationToken cancellationToken, JObject json)
        {
            await WriteHeaderAsync(json);

            var line = BuildLine(_jsonToCsvMapping, json);
            await _streamWriter.WriteLineAsync(line);
        }

        public void Dispose()
        {
            _streamWriter.Flush();
            _streamWriter.Close();
            _streamWriter.Dispose();
            _streamWriter = null;
        }

        private void InitCsvFile(FileSystemInfo fileInfo, IConsole console)
        {
            console.WriteLine($"CSV file: {fileInfo.FullName}");
            if (File.Exists(fileInfo.FullName)) console.WriteLine("The file is already exists and will be rewritten");
            _streamWriter = File.CreateText(fileInfo.FullName);
            _streamWriter.AutoFlush = true;
        }

        private void InitMapping(TopicMappingConfiguration mapping)
        {
            if (!mapping.Mapping.Any()) return;
            _jsonToCsvMapping = mapping.Mapping;
            _csvHeader = string.Join(CSV_VALUE_SEPARATOR, _jsonToCsvMapping.Keys);
        }

        private string BuildLine(IDictionary<string, string> mapping, JObject json)
        {
            var dic = json.Flatten();
            var lineDic = new Dictionary<string, object>();
            foreach (var mappingKey in mapping.Keys)
            {
                var mappedField = mapping[mappingKey];
                if (!dic.TryGetValue(mappedField, out var value)) continue;
                
                var strValue = Convert.ToString(value) ?? string.Empty;
                if (value != null && value.GetType().IsPrimitive)
                {
                    lineDic.Add(mappingKey, strValue);
                }
                else
                {
                    // replace some system type names of empty sequences with blank values
                    if (CSV_VALUE_REPLACEMENTS.Contains(strValue)) strValue = string.Empty;
                    
                    // use sortable format for date/time values (should be in UTC)
                    if (value is DateTime time) strValue = time.ToString("yyyy-MM-dd HH:mm:ss");
                            
                    var csvValue = $"{CSV_VALUE_WRAPPER}{strValue}{CSV_VALUE_WRAPPER}";
                    lineDic.Add(mappingKey, csvValue);
                }                
            }

            return string.Join(CSV_VALUE_SEPARATOR, lineDic.Values);
        }

        private async Task WriteHeaderAsync(JObject json)
        {
            if (!_isCsvHeaderWrittenToFile)
            {
                if (_jsonToCsvMapping == null || !_jsonToCsvMapping.Any())
                {
                    var mapping = new TopicMappingConfiguration(json);
                    InitMapping(mapping);
                }

                await _streamWriter.WriteLineAsync(_csvHeader);
                _isCsvHeaderWrittenToFile = true;
            }
        }
    }
}