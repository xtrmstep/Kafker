using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafker.Configurations;
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

        public static void Unsubscribe(IConsumer<Ignore, string> consumer, IConsole console)
        {
            consumer.Unsubscribe();
            console.WriteLine("Consumer unsubscribed");
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
    }
}