using System;
using System.Collections.Generic;
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

        public static async Task<KafkaTopicConfiguration> ConstructConfiguration(Dictionary<string, string> argumentList, KafkerSettings settings)
        {
            uint EVENTS_TO_READ_DEFAULT = 0;
            var OFFSET_KIND_DEFAULT = OffsetKind.Latest;
            
            var topicConfig = new KafkaTopicConfiguration()
            {
                Brokers = argumentList.ContainsKey("broker") ? new[] {argumentList["broker"]} : settings.Brokers,
                Topic = argumentList.ContainsKey("topic") ? argumentList["topic"] : null,
                OffsetKind = argumentList.ContainsKey("offset") ? (OffsetKind) Enum.Parse(typeof(OffsetKind), argumentList["offset"],true) : OFFSET_KIND_DEFAULT,
                EventsToRead = argumentList.ContainsKey("number") ? uint.Parse(argumentList["number"]) : EVENTS_TO_READ_DEFAULT
            };

            return topicConfig;
        }
    }
}