using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using JsonFlatten;
using Kafker.Configurations;
using Kafker.Csv;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafker.Helpers
{
    public static class ExtractorHelper
    {
        public static async Task<KafkaTopicConfiguration> ReadConfigurationAsync(string topic, KafkerSettings setting, IConsole console)
        {
            var path = Path.Combine(setting.ConfigurationFolder, $"{topic}.cfg");
            if (!File.Exists(path))
            {
                await console.Error.WriteLineAsync($"Cannot read the configuration file: {path}");
                throw new ApplicationException($"Cannot load configuration for topic '{topic}'");
            }

            var text = await File.ReadAllTextAsync(path);
            var topicConfiguration = JsonConvert.DeserializeObject<KafkaTopicConfiguration>(text);

            return topicConfiguration;
        }

        public static async Task<TopicMappingConfiguration> ReadMappingConfigurationAsync(string topic, KafkerSettings setting, IConsole console)
        {
            var path = Path.Combine(setting.ConfigurationFolder, $"{topic}.map");
            if (!File.Exists(path))
            {
                await console.Error.WriteLineAsync($"Cannot read the map file: {path}");
                throw new ApplicationException($"Cannot load mapping for topic '{topic}'");
            }

            var text = await File.ReadAllTextAsync(path);
            var topicMapping = JsonConvert.DeserializeObject<TopicMappingConfiguration>(text);

            return topicMapping;
        }

        public static IConsumer<Ignore, string> CreateKafkaTopicConsumer(KafkaTopicConfiguration config, IConsole console)
        {
            var dt = DateTimeOffset.Now;
            var consumerGroupTag = $"{dt:yyyyMMdd}_{dt:hhmmss}";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = string.Join(',', config.Brokers),
                GroupId = $"kafka_topic_extractor_{consumerGroupTag}",
                EnableAutoCommit = true,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = config.OffsetKind == OffsetKind.Earliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
                EnablePartitionEof = true
            };
            var consumerBuilder = new ConsumerBuilder<Ignore, string>(consumerConfig);
            var consumer = consumerBuilder.Build();
            consumer.Subscribe(config.Topic);

            console.WriteLine($"Created a consumer: {consumerConfig.GroupId}");
            console.WriteLine($"    brokers: {consumerConfig.BootstrapServers}");
            console.WriteLine($"    autoOffsetReset: {consumerConfig.AutoOffsetReset}");
            console.WriteLine($"    topic: {config.Topic}");

            return consumer;
        }

        public static FileInfo GetDestinationCsvFilename(string topic, KafkerSettings setting, IFileTagProvider fileTagProvider)
        {
            var tag = fileTagProvider.GetTag();
            var filePath = Path.Combine(setting.Destination, $"{topic}_{tag}.csv");
            var fileInfo = new FileInfo(filePath);
            return fileInfo;
        }

        public static void Unsubscribe(IConsumer<Ignore, string> consumer, IConsole console)
        {
            consumer.Unsubscribe();
            console.WriteLine("Consumer unsubscribed");
        }

        public static ICsvFileWriter CreateCsvFileWriter(FileInfo destinationCsvFile, TopicMappingConfiguration mapping, IConsole console)
        {
            return new CsvFileWriter(destinationCsvFile, mapping, console);
        }

        public static ICsvFileReader CreateCsvFileReader(FileInfo sourceCsvFile, TopicMappingConfiguration mapping, IConsole console)
        {
            return new CsvFileReader(sourceCsvFile, mapping, console);
        }

        public static IProducer<string, string> CreateKafkaTopicProducer(KafkaTopicConfiguration config, IConsole console)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = string.Join(',', config.Brokers)
            };
            var producerBuilder = new ProducerBuilder<string, string>(producerConfig);
            var producer = producerBuilder.Build();
            
            console.WriteLine($"Created a producer:");
            console.WriteLine($"    brokers: {producerConfig.BootstrapServers}");
            console.WriteLine($"    topic: {config.Topic}");
            
            return producer;
        }

        public static async Task ProduceAsync(IProducer<string,string> producer, KafkaTopicConfiguration cfg, JToken json)
        {
            var message = new Message<string, string>
            {
                Key = string.Empty,
                Value = json.ToString(Formatting.None)
            };
            await producer.ProduceAsync(cfg.Topic, message);
        }

        public static FlattenBugger CreateFlattenBuffer()
        {
            return new FlattenBugger();
        }
    }

    public class FlattenBugger
    {
        private DataTable _tbl = new DataTable();
        
        public void Add(JObject json)
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
            CSVLibraryAK.CSVLibraryAK.Export(destinationCsvFile.FullName, _tbl);
        }

        public JObject[] GetJsonRecords()
        {
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
        
        public async Task LoadFromFileAsync(FileInfo destinationCsvFile)
        {
            await Task.Yield();
            _tbl = CSVLibraryAK.CSVLibraryAK.Import(destinationCsvFile.FullName, true);
        }
    }
}