using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JsonFlatten;
using KafkaTopicExtractor.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Csv
{
    public class CsvFileIo : ICsvFileWriter
    {
        private const string CSV_SEPARATOR = ";";
        private string _csvHeader;
        private bool _isCsvHeaderWrittenToFile;
        private IDictionary<string, string> _jsonToCsvMapping;
        private StreamWriter _streamWriter;

        public CsvFileIo(FileSystemInfo fileInfo, TopicMappingConfiguration mapping, IConsole console)
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
            _streamWriter.Close();
            _streamWriter.Dispose();
            _streamWriter = null;
        }

        private void InitCsvFile(FileSystemInfo fileInfo, IConsole console)
        {
            console.WriteLine($"CSV file: {fileInfo.FullName}");
            if (File.Exists(fileInfo.FullName)) console.WriteLine("The file is already exists and will be rewritten");
            _streamWriter = File.CreateText(fileInfo.FullName);
        }

        private void InitMapping(TopicMappingConfiguration mapping)
        {
            if (!mapping.Mapping.Any()) return;
            _jsonToCsvMapping = mapping.Mapping;
            _csvHeader = string.Join(CSV_SEPARATOR, _jsonToCsvMapping.Keys);
        }

        private string BuildLine(IDictionary<string, string> mapping, JObject json)
        {
            var dic = json.Flatten();
            var lineDic = new Dictionary<string, object>();
            foreach (var mappingKey in mapping.Keys)
            {
                var strValue = string.Empty;
                var mappedField = mapping[mappingKey];
                if (dic.TryGetValue(mappedField, out var value)) strValue = Convert.ToString(value);
                lineDic.Add(mappingKey, strValue);
            }

            return string.Join(CSV_SEPARATOR, lineDic.Values);
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