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