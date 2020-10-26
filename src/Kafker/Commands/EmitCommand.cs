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
        private readonly IFileHandler _fileHandler;

        
        public EmitCommand(IConsole console, KafkerSettings settings, IProducerFactory producerFactory,
                            IFileHandler fileHandler)
        {
            _console = console;
            _settings = settings;
            _producerFactory = producerFactory;
            _fileHandler = fileHandler;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);

            var producedEvents = 0;
            using var topicProducer = _producerFactory.Create(cfg);
            try
            {
                var result = await _fileHandler.LoadFromFileAsync(fileName);

                float total = 0;
                float idx = 0;
                foreach (var record in result)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await topicProducer.ProduceAsync(record);
                    producedEvents++;
                    await _console.Out.WriteAsync($"\rproduced {++idx / total * 100:f2}% [{idx:f0}/{total:f0}]");
                }
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}