using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafker.Commands
{
    public class ConfigCommand : IConfigCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public ConfigCommand(IConsole console, KafkerSettings settings)
        {
            _console = console;
            _settings = settings;
        }

        public async Task<int> ShowConfigurationAsync()
        {
            var json = JObject.FromObject(_settings);
            var txt = json.ToString(Formatting.Indented);
            await _console.Out.WriteLineAsync(txt);
            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<int> SetConfigurationFolderAsync(string value)
        {
            await _console.Out.WriteLineAsync($"set configuration {value}");
            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<int> SetDestinationFolderAsync(string value)
        {
            await _console.Out.WriteLineAsync($"set destination {value}");
            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
        }
    }
}