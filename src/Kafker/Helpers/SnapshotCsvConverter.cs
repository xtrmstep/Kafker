﻿using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JsonFlatten;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public class SnapshotCsvConverter
    {
        private readonly IConsole _console;
        private DataTable _tbl = new DataTable();
        private readonly KafkaTopicConfiguration _mapConfig;

        public SnapshotCsvConverter(IConsole console, KafkaTopicConfiguration mapConfig)
        {
            _console = console;
            _mapConfig = mapConfig;
        }

        private void Convert(JObject json)
        {
            var dic = json.Flatten();
            var columns = new List<string>(dic.Keys);
            var row = _tbl.NewRow();
            // add missing columns
            foreach (var column in columns)
            {
                var mapNotNullAndKeyExists = _mapConfig.Mapping != null && _mapConfig.Mapping.ContainsKey(column);
                var renamedColumnName = column;
                if (mapNotNullAndKeyExists)
                {
                    renamedColumnName = _mapConfig.Mapping[column];
                }

                if (_tbl.Columns.Contains(renamedColumnName)) continue;

                var shouldAddColumn = _mapConfig.Mapping == null || mapNotNullAndKeyExists;
                if (!shouldAddColumn) continue;

                var dataColumn = new DataColumn(renamedColumnName, typeof(object));
                _tbl.Columns.Add(dataColumn);

                row[renamedColumnName] = dic[column];
            }

            _tbl.Rows.Add(row);
        }

        private async Task SaveToFileAsync(FileInfo destinationCsvFile)
        {
            await Task.Yield();
            CSVLibraryAK.Core.CSVLibraryAK.Export(destinationCsvFile.FullName, _tbl);
        }

        private async Task LoadFromFileAsync(string sourceFile)
        {
            await Task.Yield();
            var list = new List<JObject>();
            var lines = await File.ReadAllLinesAsync(sourceFile);
            foreach (var line in lines)
            {
                var pair = line.Split("|");
                //var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                var record = pair[1].Substring(1, pair[1].Length - 2);
                JObject json = JObject.Parse(record);
                list.Add(json);
            }

            ConvertListToDataTable(list);
        }

        private void ConvertListToDataTable(List<JObject> list)
        {
            foreach (var item in list)
            {
                Convert(item);
            }
        }

        public async Task ConvertAndSaveAsync(string fileName)
        {
            var sourceFile = fileName;
            var destinationFile = new FileInfo($"{sourceFile.Replace(".dat", "")}.csv");
            await LoadFromFileAsync(fileName);
            await SaveToFileAsync(destinationFile);
        }
    }
}