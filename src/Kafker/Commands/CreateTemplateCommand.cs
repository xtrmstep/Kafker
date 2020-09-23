using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Kafker.Commands
{
    public class CreateTemplateCommand : ICreateTemplateCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public CreateTemplateCommand(IConsole console, IOptions<KafkerSettings> settings)
        {
            _console = console;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string templateName)
        {
            templateName ??= "template";
            await CreateCfgTemplateAsync(templateName);
            await CreateMapTemplateAsync(templateName);

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
                Topic = templateName,
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