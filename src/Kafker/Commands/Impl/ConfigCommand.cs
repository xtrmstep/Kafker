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

        public ListCommand(IConsole console, KafkerSettings settings)
        {
            _console = console;
            _settings = settings;
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

            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
        }
    }
}