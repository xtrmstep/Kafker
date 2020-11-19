using System;
using System.Data;
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
        public static async Task<KafkaTopicConfiguration> ReadConfigurationAsync(string configName, KafkerSettings setting, IConsole console)
        {
            var path = Path.Combine(setting.ConfigurationFolder, $"{configName}.cfg");
            if (!File.Exists(path))
            {
                await console.Error.WriteLineAsync($"Cannot read the configuration file: {path}");
                throw new ApplicationException($"Cannot load configuration for topic '{configName}'");
            }


            var text = await File.ReadAllTextAsync(path);
            var topicConfiguration = JsonConvert.DeserializeObject<KafkaTopicConfiguration>(text);

            return topicConfiguration;
        }

        public static async Task<KafkaTopicConfiguration> GetConfiguration(KafkerSettings settings,
            string configName, string brokers, string topic, uint? eventToRead, OffsetKind? offset)
        {
            
            var conf = string.IsNullOrWhiteSpace(configName) 
                ? await ReadConfigurationAsync(configName, settings, PhysicalConsole.Singleton)
                : new KafkaTopicConfiguration
                {
                    Brokers = settings.Brokers,
                    Topic = string.Empty,
                    EventsToRead = 0,
                    OffsetKind = OffsetKind.Latest
                };
            
            if (!string.IsNullOrWhiteSpace(brokers))
                conf.Brokers = brokers.Split(",");

            if (!string.IsNullOrWhiteSpace(topic))
                conf.Topic = topic;
            
            if (eventToRead.HasValue)
                conf.EventsToRead = eventToRead.Value;
            
            if (offset.HasValue)
                conf.OffsetKind = offset.Value;
            
            // validation
            if (!conf.Brokers.Any()) throw new ArgumentException("Brokers are required", nameof(KafkaTopicConfiguration.Brokers));
            if (string.IsNullOrWhiteSpace(conf.Topic)) throw new ArgumentException("Topic is required", nameof(KafkaTopicConfiguration.Topic));
            
            return conf;
        }

    }
}