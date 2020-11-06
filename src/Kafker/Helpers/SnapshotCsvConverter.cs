﻿using System.Collections.Generic;
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
        private DataTable _tbl = new DataTable();
        private readonly KafkaTopicConfiguration _mapConfig;

        public SnapshotCsvConverter(KafkaTopicConfiguration mapConfig)
        {
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
                var mapNotNullAndKeyExists = _mapConfig.Mapping.Any() && _mapConfig.Mapping.ContainsKey(column);
                var renamedColumnName = column;
                if (mapNotNullAndKeyExists)
                {
                    renamedColumnName = _mapConfig.Mapping[column];
                }

                if (!_tbl.Columns.Contains(renamedColumnName))
                {
                    var shouldAddColumn = !_mapConfig.Mapping.Any() || mapNotNullAndKeyExists;
                    if (!shouldAddColumn) continue;

                    var dataColumn = new DataColumn(renamedColumnName, typeof(object));
                    _tbl.Columns.Add(dataColumn);
                }

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
                JObject json = JsonConvert.DeserializeObject<JObject>(record, new JsonSerializerSettings {DateParseHandling = DateParseHandling.None});
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
            var destinationFile = new FileInfo($"{fileName.Replace(".dat", "")}.csv");
            await LoadFromFileAsync(fileName);
            await SaveToFileAsync(destinationFile);

        }
    }
}