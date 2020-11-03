using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public class ConvertCommand : IConvertCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public ConvertCommand(IConsole console, KafkerSettings settings)
        {
            _console = console;
            _settings = settings;
        
        }
        
        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string fileName, string topic)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            try
            {
                var csvConverter = new SnapshotCsvConverter(_console,cfg,_settings);
                await csvConverter.ConvertAndSaveAsync(fileName);
        
            }
            finally
            {
                    await _console.Out.WriteLineAsync($"\r\nConversion completed");
            }
            
            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}