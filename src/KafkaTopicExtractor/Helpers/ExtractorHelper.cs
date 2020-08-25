using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaTopicExtractor.Configurations;
using KafkaTopicExtractor.Csv;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Helpers
{
    public static class ExtractorHelper
    {
        public static async Task<KafkaTopicConfiguration> ReadConfigurationAsync(string topic, KafkaTopicExtractorSettings setting, IConsole console)
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

        public static async Task<TopicMappingConfiguration> ReadMappingConfigurationAsync(string topic, KafkaTopicExtractorSettings setting, IConsole console)
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

            var conf = new ConsumerConfig
            {
                BootstrapServers = string.Join(',', config.Brokers),
                GroupId = $"kafka_topic_extractor_{consumerGroupTag}",
                EnableAutoCommit = true,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = config.OffsetKind == OffsetKind.Earliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
                EnablePartitionEof = true
            };
            var consumerBuilder = new ConsumerBuilder<Ignore, string>(conf);
            var consumer = consumerBuilder.Build();
            consumer.Subscribe(config.Topic);

            console.Out.WriteLineAsync($"Created a consumer: {conf.GroupId}");
            console.Out.WriteLineAsync($"    brokers: {conf.BootstrapServers}");
            console.Out.WriteLineAsync($"    autoOffsetReset: {conf.AutoOffsetReset}");
            console.Out.WriteLineAsync($"    topic: {config.Topic}");

            return consumer;
        }

        public static FileInfo GetDestinationCsvFilename(string topic, KafkaTopicExtractorSettings setting, IFileTagProvider fileTagProvider)
        {
            var tag = fileTagProvider.GetTag();
            var filePath = Path.Combine(setting.Destination, $"{topic}_{tag}.csv");
            var fileInfo = new FileInfo(filePath);
            return fileInfo;
        }

        public static async Task WriteToCsvAsync(FileInfo destinationFileName, JObject json, TopicMappingConfiguration mapping)
        {
            await Task.Yield();
        }

        public static void Unsubscribe(IConsumer<Ignore, string> consumer, IConsole console)
        {
            consumer.Unsubscribe();
            console.Out.WriteLineAsync("Consumer unsubscribed");
        }

        public static ICsvFileWriter CreateCsvFileWriter(FileInfo destinationCsvFile, TopicMappingConfiguration mapping, IConsole console)
        {
            return new CsvFileIo(destinationCsvFile, mapping, console);
        }
    }
}