using System.IO;
using System.Net;
using System.Net.Security;
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
            try
            {
                KafkaTopicConfiguration cfg;
                if (topic == null)
                {
                    cfg = null;
                }
                else
                {
                    cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
                }
                
                var isFound = true;
                var snapshotFilePath = fileName;
                if (!File.Exists(snapshotFilePath))
                {
                    snapshotFilePath = Path.Combine(_settings.Destination, fileName);
                    if (!File.Exists(snapshotFilePath))
                    {
                        isFound = false;
                        await _console.Out.WriteAsync($"File cannot be found : {fileName}");
                    }
                }
                if (isFound)
                {
                    var csvConverter = new SnapshotCsvConverter(cfg);
                    await csvConverter.ConvertAndSaveAsync(snapshotFilePath);
                    await _console.Out.WriteLineAsync($"\r\nConversion completed");
                }
                
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\n");
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}