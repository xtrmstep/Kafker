using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafker.Configurations;
using Kafker.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace Kafker.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public EmitCommand(IConsole console, IOptions<KafkerSettings> settings)
        {
            _console = console;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var mapping = await ExtractorHelper.ReadMappingConfigurationAsync(topic, _settings, _console);

            using var topicProducer = ExtractorHelper.CreateKafkaTopicProducer(cfg, _console);
            var sourceCsvFile = new FileInfo(fileName);
            var csvFileReader = ExtractorHelper.CreateCsvFileReader(sourceCsvFile, mapping, _console);

            var producedEvents = 0;
            try
            {
                producedEvents = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    var json = await csvFileReader.ReadLineAsync(cancellationToken);
                    if (json == null) break;
                    
                    await ExtractorHelper.ProduceAsync(topicProducer, cfg, json);
                    producedEvents++;
                    await _console.Out.WriteAsync($".");
                    await Task.Delay(5, cancellationToken);
                }
            }
            finally
            {
                await _console.Out.WriteLineAsync($"Produced {producedEvents} events"); 
            }

            return 1; // ok
        }
    }
}