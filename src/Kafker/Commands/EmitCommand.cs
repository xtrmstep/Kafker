using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;
        private readonly IProducerFactory _producerFactory;
        private readonly IEventsEmitter _eventsEmitter;

        public EmitCommand(IConsole console, KafkerSettings settings, IProducerFactory producerFactory, IEventsEmitter eventsEmitter)
        {
            _console = console;
            _settings = settings;
            _producerFactory = producerFactory;
            _eventsEmitter = eventsEmitter;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, KafkaTopicConfiguration kafkaTopicConfiguration, string fileName)
        {
            try
            {
                var snapshotFilePath = ExtractorHelper.GetAbsoluteFilePath(fileName, _settings.Destination);
                if (snapshotFilePath == null)
                    throw new FileNotFoundException("File cannot be found", fileName);
                
                await _eventsEmitter.EmitEvents(cancellationToken, kafkaTopicConfiguration, snapshotFilePath);
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (FileNotFoundException err)
            {
                await _console.Error.WriteAsync($"{err.Message}: {err.FileName}");
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
            catch (Exception err)
            {
                await _console.Error.WriteLineAsync($"\r\nError: {err.Message}");
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
        }
    }
}
