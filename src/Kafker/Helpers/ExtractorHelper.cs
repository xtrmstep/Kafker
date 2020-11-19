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

        public static async Task<KafkaTopicConfiguration> ConstructConfiguration(Dictionary<string,string> argumentList, KafkerSettings settings)
        {
            const uint EVENTS_TO_READ_DEFAULT = 0;
            const OffsetKind OFFSET_KIND_DEFAULT = OffsetKind.Latest;
            
            var topicConfig = new KafkaTopicConfiguration()
            {
                Brokers = argumentList.ContainsKey("broker") ? new[] {argumentList["broker"]} : settings.Brokers,
                Topic = argumentList["topic"],
                OffsetKind = argumentList.ContainsKey("offset") ? (OffsetKind) Enum.Parse(typeof(OffsetKind), argumentList["offset"], true) : OFFSET_KIND_DEFAULT,
                EventsToRead = argumentList.ContainsKey("number") ? uint.Parse(argumentList["number"]) : EVENTS_TO_READ_DEFAULT
            };

            return topicConfig;
        }

        public static async Task<KafkaTopicConfiguration> OverrideConfiguration(KafkaTopicConfiguration configuration, Dictionary<string,string> argumentList, KafkerSettings settings)
        {
            var topicConfig = new KafkaTopicConfiguration()
            {
                Brokers = argumentList.ContainsKey("broker") ? new[] {argumentList["broker"]} : configuration.Brokers,
                Topic = argumentList.ContainsKey("topic") ? argumentList["topic"] : configuration.Topic,
                OffsetKind = argumentList.ContainsKey("offset") ? (OffsetKind) Enum.Parse(typeof(OffsetKind), argumentList["offset"], true) : configuration.OffsetKind,
                EventsToRead = argumentList.ContainsKey("number") ? uint.Parse(argumentList["number"]) : configuration.EventsToRead
            };

            return topicConfig;
        }

        public static Dictionary<string,string> GetOptionList(List<CommandOption> commandOptions)
        {
            var argumentList = new Dictionary<string, string>();
            foreach (var item in commandOptions.Where(item => item.HasValue()))
            {
                argumentList.Add(item.LongName, item.Value());
            }

            return argumentList;
        }

        public static async Task<KafkaTopicConfiguration> GetConfiguration(List<CommandOption> commandOptions,KafkerSettings settings)
        {
            
            var argumentList = GetOptionList(commandOptions);
            string value = "";
            var getConfgArg = argumentList.TryGetValue("config", out value);
            KafkaTopicConfiguration conf = value != null ? await ReadConfigurationAsync(value, settings, PhysicalConsole.Singleton) : null;
            
            if (value != null && argumentList.Count > 1)
            {
                conf = await OverrideConfiguration(conf, argumentList, settings);
            }
            
            if (value == null && argumentList.Count >= 1)
            {
                conf = await ConstructConfiguration(argumentList, settings);
            }

            return conf;
        }

    }
}