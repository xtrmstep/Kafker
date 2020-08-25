using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KafkaTopicExtractor.Configurations;
using KafkaTopicExtractor.Csv;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json.Linq;
using Xunit;

namespace KafkaTopicExtractor.Tests.Csv
{
    public class CsvFileIoTests : IDisposable
    {
        private readonly FileInfo _fileInfo = new FileInfo(Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, ".\\sample.csv")));
        private readonly string _sampleJsonFile = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, ".\\sample.json"));

        /// <inheritdoc />
        public void Dispose()
        {
            File.Delete(_fileInfo.FullName);
        }

        [Fact]
        public async Task When_mapping_empty_should_write_full_flatten_json_to_csv()
        {
            var expected = new[]
            {
                "Name;Color;Attributes[0].Name;Attributes[0].Value;Attributes[1].Name;Attributes[1].Value[0].Type;Attributes[1].Value[0].Length",
                "Fish;Silver;Environment;Aquatic;Parts;fin;3"
            };
            var emptyMapping = new TopicMappingConfiguration();
            using (var csvFileIo = new CsvFileWriter(_fileInfo, emptyMapping, PhysicalConsole.Singleton))
            {
                var json = JObject.Parse(await File.ReadAllTextAsync(_sampleJsonFile));
                await csvFileIo.WriteAsync(CancellationToken.None, json);
            }

            var csvLines = await File.ReadAllLinesAsync(_fileInfo.FullName);
            csvLines.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public async Task When_mapping_single_field_should_write_single_field_to_csv()
        {
            var expected = new[]
            {
                "Attributes[1].Value[0].Length",
                "3"
            };
            var mapping = new TopicMappingConfiguration();
            mapping.Mapping.Add("Attributes[1].Value[0].Length", "Attributes[1].Value[0].Length");
            using (var csvFileIo = new CsvFileWriter(_fileInfo, mapping, PhysicalConsole.Singleton))
            {
                var json = JObject.Parse(await File.ReadAllTextAsync(_sampleJsonFile));
                await csvFileIo.WriteAsync(CancellationToken.None, json);
            }

            var csvLines = await File.ReadAllLinesAsync(_fileInfo.FullName);
            csvLines.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public async Task When_mapping_renamed_field_should_write_original_field_value_to_csv()
        {
            var expected = new[]
            {
                "Mapped_Length",
                "3"
            };
            var mapping = new TopicMappingConfiguration();
            mapping.Mapping.Add("Mapped_Length", "Attributes[1].Value[0].Length");
            using (var csvFileIo = new CsvFileWriter(_fileInfo, mapping, PhysicalConsole.Singleton))
            {
                var json = JObject.Parse(await File.ReadAllTextAsync(_sampleJsonFile));
                await csvFileIo.WriteAsync(CancellationToken.None, json);
            }

            var csvLines = await File.ReadAllLinesAsync(_fileInfo.FullName);
            csvLines.Should().BeEquivalentTo(expected);
        }
    }
}