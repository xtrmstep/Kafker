using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JsonFlatten;
using KafkaTopicExtractor.Configurations;
using KafkaTopicExtractor.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.VisualBasic;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Csv
{
    public class CsvFileIo : ICsvFileWriter
    {
        private readonly TopicMappingConfiguration _mapping;
        private readonly IConsole _console;
        private readonly string _csvSeparator = ";";
        private StreamWriter _streamWriter;
        private string _csvHeader;
        private bool _isCsvHeaderWrittenToFile = false;
        private IDictionary<string, string> _jsonToCsvMapping;
        private bool _isHeaderInitialized = false;

        public CsvFileIo(FileSystemInfo fileInfo, TopicMappingConfiguration mapping, IConsole console)
        {
            _mapping = mapping;
            _console = console;
            
            InitCsvFile(fileInfo);
            InitMapping(_mapping);            
        }

        private void InitCsvFile(FileSystemInfo fileInfo)
        {
            _console.Out.WriteLine($"CSV file: {fileInfo.FullName}");
            if (File.Exists(fileInfo.FullName))
            {
                _console.Out.WriteLine("The file is already exists and will be rewritten");
            }
            _streamWriter = File.CreateText(fileInfo.FullName);
        }

        private void InitMapping(TopicMappingConfiguration mapping)
        {
            if (!mapping.Mapping.Any()) return;
            _jsonToCsvMapping = mapping.Mapping;
            _csvHeader = string.Join(_csvSeparator, _jsonToCsvMapping.Keys);
            _isHeaderInitialized = true;
        }

        public async Task WriteAsync(CancellationToken cancellationToken, JObject json)
        {
            await WriteHeaderAsync(json);

            var line = BuildLine(_jsonToCsvMapping, json);
            await _streamWriter.WriteLineAsync(line);
        }

        private string BuildLine(IDictionary<string, string> mapping, JObject json)
        {
            var dic = json.Flatten();
            var lineDic = new Dictionary<string, object>();
            foreach (var mappingKey in mapping.Keys)
            {
                var strValue = string.Empty;
                if (dic.TryGetValue(mappingKey, out var value))
                {
                    strValue = Convert.ToString(value);
                }
                lineDic.Add(mappingKey, strValue);
            }

            return string.Join(_csvSeparator, lineDic.Values);
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
            }
        }

        public void Dispose()
        {
            _streamWriter.Close();
            _streamWriter.Dispose();
            _streamWriter = null;
        }
    }
}