using System;
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


        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string fileName, KafkaTopicConfiguration topicConfiguration)
        {
            try
            {
                var snapshotFilePath = ExtractorHelper.GetAbsoluteFilePath(fileName, _settings.Destination);
                if (snapshotFilePath == null)
                    throw new FileNotFoundException("File cannot be found", fileName);

                var csvConverter = new SnapshotCsvConverter(topicConfiguration);
                await csvConverter.ConvertAndSaveAsync(snapshotFilePath);
                await _console.Out.WriteLineAsync($"\r\nConversion completed");
                
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (FileNotFoundException err)
            {
                await _console.Error.WriteAsync($"{err.Message}: {err.FileName}");
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\n");
            }            
        }
    }
}