using System;
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
                await _eventsEmitter.EmitEvents(cancellationToken, kafkaTopicConfiguration, fileName);
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (Exception err)
            {
                await _console.Error.WriteLineAsync($"\r\nError: {err.Message}");
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
        }
    }
}
