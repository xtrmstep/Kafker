using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
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
    public abstract class CsvFileBase
    {
        protected const string CSV_SEPARATOR = ";";

    }
    
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

    public class CsvFileReader : CsvFileBase, ICsvFileReader
    {
        private readonly IConsole _console;
        private StreamReader _streamReader;
        private string[] _fileFields;
        private IDictionary<string, string> _destinationFields;

        public CsvFileReader(FileSystemInfo fileInfo, TopicMappingConfiguration mapping, IConsole console)
        {
            _console = console;
            InitCsvFile(fileInfo);
            InitMapping(mapping);
        }

        private void InitMapping(TopicMappingConfiguration mapping)
        {
            var headerLine = _streamReader.ReadLine();
            Contract.Assert(headerLine != null, "Cannot read line or it's empty");
            Contract.Assert(headerLine.Length > 0, "Cannot read line or it's empty");
            
            _fileFields = headerLine.Split(CSV_SEPARATOR);
            _destinationFields = (mapping.Mapping!= null && mapping.Mapping.Any() ? mapping.Mapping : null) 
                                 ?? _fileFields.ToDictionary(f => f, f => f);
        }

        private void InitCsvFile(FileSystemInfo fileInfo)
        {
            _console.WriteLine($"CSV file: {fileInfo.FullName}");
            if (!File.Exists(fileInfo.FullName))
            {
                var message = $"Cannot find or read the file: {fileInfo.FullName}";
                _console.WriteLine(message);
                throw new ApplicationException(message);
            }
            _streamReader = File.OpenText(fileInfo.FullName);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _streamReader.Close();
            _streamReader.Dispose();
            _streamReader = null;
        }

        /// <inheritdoc />
        public async Task<JObject> ReadLineAsync(CancellationToken cancellationToken)
        {
            var line = await _streamReader.ReadLineAsync();
            if (line == null) return null;

            var values = line.Split(CSV_SEPARATOR);
            var dic = new Dictionary<string, object>();
            for (var idx = 0; idx < _fileFields.Length; idx++)
            {
                var fileField = _fileFields[idx];
                var value = values[idx];
                dic.Add(_destinationFields[fileField], value);
            }

            return dic.Unflatten();
        }
    }
}