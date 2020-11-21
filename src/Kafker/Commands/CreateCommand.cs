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

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string configName)
        {
            configName ??= "template";
            await CreateConfigurationFileAsync(configName, cancellationToken);

            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
        }

        private async Task CreateConfigurationFileAsync(string configName, CancellationToken cancellationToken)
        {
            var path = GetFilename(configName, "cfg");
            var brokerAddress = $@"[""{string.Join("\",\"", _settings.Brokers)}""]";
            var template = $@"{{
    ""Brokers"" : {brokerAddress},
    ""Topic"" : ""{configName}"",
    ""EventsToRead"" : 0|N,
    ""OffsetKind"" : ""Latest|Earliest"",
    ""Mapping"" : {{
        ""Property"" : ""destination_property_name"",
        ""Node.Property"" : ""destination_property_of_nested_type"",
        ""Node.Array[1]"" : ""destination_property_of_array_element""   
        }}
}}";
            if (File.Exists(path))
            {
                await _console.Out.WriteAsync($"File already exists [{path}]. Overwrite? [y|n]");
                var chars = new char[1];
                await _console.In.ReadAsync(chars, cancellationToken);
                if (chars[0].ToString().ToUpper() != "Y")
                {
                    await _console.Out.WriteLineAsync($"Cancelling operation");
                    return;
                }
            }
            await File.WriteAllTextAsync(path, template, cancellationToken);
            await _console.Out.WriteLineAsync($"Created: {path}");
        }

        private string GetFilename(string templateName, string extension)
        {
            return Path.Combine(_settings.ConfigurationFolder, $"{templateName}.{extension}");
        }
    }
}