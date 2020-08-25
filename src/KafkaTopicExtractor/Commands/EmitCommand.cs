using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaTopicExtractor.Configurations;
using KafkaTopicExtractor.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkaTopicExtractorSettings _settings;

        public EmitCommand(IConsole console, IOptions<KafkaTopicExtractorSettings> settings)
        {
            _console = console;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var mapping = await ExtractorHelper.ReadMappingConfigurationAsync(topic, _settings, _console);

            using var topicProducer = ExtractorHelper.CreateKafkaTopicProducer(cfg, _console);
            var csvFileReader = ExtractorHelper.CreateCsvFileReader(new FileInfo(fileName), mapping, _console);

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