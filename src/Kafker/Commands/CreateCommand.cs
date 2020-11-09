using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public class CreateCommand : ICreateCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;
        
            public CreateCommand(IConsole console, KafkerSettings settings)
        {
            _console = console;
            _settings = settings;
            
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string templateName)
        {
            templateName ??= "template";
            await CreateCfgMapTemplateAsync(templateName);

            return await Task.FromResult(0).ConfigureAwait(false);
        }

        private async Task CreateCfgMapTemplateAsync(string templateName)
        {
            var path = GetFilename(templateName, "cfg");
            var brokerAddress = $@"[""{string.Join("\",\"", _settings.Brokers)}""]";
            var template = @"{
    Brokers : {broker-address},
    Topic : ""topic-name"",
    EventsToRead : 0|N,
    OffsetKind : ""Latest|Earliest"",
    Mapping : {
        ""Property"" : ""destination_property_name"",
        ""Node.Property"" : ""destination_property_of_nested_type"",
        ""Node.Array[1]"" : ""destination_property_of_array_element""   
        }
}";
            string templateWithConfig = template.Replace("{broker-address}", brokerAddress);    
            
            await File.WriteAllTextAsync(path, templateWithConfig);
            
        }

        private string GetFilename(string templateName, string extension)
        {
            return Path.Combine(_settings.ConfigurationFolder, $"{templateName}.{extension}");
        }
    }
}