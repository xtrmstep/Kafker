using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Dynamitey;
using JsonFlatten;
using Kafker.Configurations;
using Kafker.Helpers;
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
        private CsvReader _csvReader;

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
            _csvReader = new CsvReader(_streamReader, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _csvReader.Dispose();
            _csvReader = null;
            
            _streamReader.Close();
            _streamReader.Dispose();
            _streamReader = null;
        }

        /// <inheritdoc />
        public async Task<JObject> ReadLineAsync(CancellationToken cancellationToken)
        {
            var record = _csvReader.GetRecord<dynamic>();
            IEnumerable<string> n = Dynamic.GetMemberNames(record);            
            if (record == null || n == null || !(n ?? new string[0]).Any()) return null;

            var na = n.ToArray();

            var dic = new Dictionary<string, object>();
            for (var idx = 0; idx < _fileFields.Length; idx++)
            {
                var fileField = _fileFields[idx];
                var value = Dynamic.InvokeGet(record, na[idx]);
                dic.Add(_destinationFields[fileField], value);
            }

            return dic.Unflatten();
        }

        /// <inheritdoc />
        public bool Read()
        {
            return _csvReader.Read();
        }
    }
}