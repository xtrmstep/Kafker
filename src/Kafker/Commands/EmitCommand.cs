using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
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

        public EmitCommand(IConsole console, KafkerSettings settings, IProducerFactory producerFactory)
        {
            _console = console;
            _settings = settings;
            _producerFactory = producerFactory;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);

            var isFound = true;
            var snapshotFilePath = fileName;
            var producedEvents = 0;
            if (!File.Exists(snapshotFilePath))
            {
                snapshotFilePath = Path.Combine(_settings.Destination, fileName);
                if (!File.Exists(snapshotFilePath))
                {
                    isFound = false;
                    await _console.Out.WriteAsync($"File cannot be found : {fileName}");
                }
            }

            using var topicProducer = _producerFactory.Create(cfg);
            if (isFound)
            {
                using (var reader = new StreamReader(snapshotFilePath))
                {
                    try
                    {
                        string line;
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            if (cancellationToken.IsCancellationRequested) break;

                            var pair = line.Split("|");
                            //var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                            var jsonText = pair[1].Substring(1, pair[1].Length - 2);
                            await topicProducer.ProduceAsync(jsonText);
                            producedEvents++;
                        }
                    }
                    finally
                    {
                        await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
                    }
                }
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}