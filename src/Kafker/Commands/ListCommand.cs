using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Kafker.Commands
{
    public class ListCommand : IListCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public ListCommand(IConsole console, IOptions<KafkerSettings> settings)
        {
            _console = console;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync()
        {
            var di = new DirectoryInfo(_settings.ConfigurationFolder);
            var cfgFiles = di.GetFiles("*.cfg");
            if (cfgFiles.Any())
            {
                foreach (var name in cfgFiles.Select(c => c.Name.Replace(c.Extension, string.Empty)).OrderBy(n => n))
                {
                    await _console.Out.WriteLineAsync(name);
                }
            }
            else
            {
                await _console.Out.WriteLineAsync("- empty- ");
            }

            return await Task.FromResult(0).ConfigureAwait(false);
        }

        private async Task CreateMapTemplateAsync(string templateName)
        {
            var path = GetFilename(templateName, "map");
            var templateObject = new TopicMappingConfiguration
            {
                Mapping = new Dictionary<string, string>
                {
                    {"destination_property_name", "Property"},
                    {"destination_property_of_nested_type", "Node.Property"},
                    {"destination_property_of_array_element", "Node.Array[1]"}
                }
            };
            await File.WriteAllTextAsync(path, JsonConvert.SerializeObject(templateObject, Formatting.Indented));
        }

        private async Task CreateCfgTemplateAsync(string templateName)
        {
            var path = GetFilename(templateName, "cfg");
            var templateObject = new KafkaTopicConfiguration
            {
                Brokers = new[] {"broker1"},
                Topic = "topic",
                EventsToRead = 100,
                OffsetKind = OffsetKind.Latest
            };
            await File.WriteAllTextAsync(path, JsonConvert.SerializeObject(templateObject, Formatting.Indented));
        }

        private string GetFilename(string templateName, string extension)
        {
            return Path.Combine(_settings.ConfigurationFolder, $"{templateName}.{extension}");
        }
    }
}