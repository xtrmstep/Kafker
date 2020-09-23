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
            
            _fileFields = headerLine.Split(CSV_VALUE_SEPARATOR);
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

            var values = line.Split(CSV_VALUE_SEPARATOR);
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