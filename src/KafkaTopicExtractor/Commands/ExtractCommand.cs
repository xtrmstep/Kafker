using System.Threading;
using System.Threading.Tasks;
using KafkaTopicExtractor.Configurations;
using KafkaTopicExtractor.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Commands
{
    public class ExtractCommand : IExtractCommand
    {
        private readonly IConsole _console;
        private readonly IFileTagProvider _fileTagProvider;
        private readonly KafkaTopicExtractorSettings _settings;

        public ExtractCommand(IConsole console, IFileTagProvider fileTagProvider, IOptions<KafkaTopicExtractorSettings> settings)
        {
            _console = console;
            _fileTagProvider = fileTagProvider;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string map)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var mapping = await ExtractorHelper.ReadMappingConfigurationAsync(map ?? topic, _settings, _console);

            var topicConsumer = ExtractorHelper.CreateKafkaTopicConsumer(cfg, _console);
            var csvFile = ExtractorHelper.CreateCsvFileWriter();
            
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = topicConsumer.Consume(cancellationToken);
                    if (consumeResult.IsPartitionEOF) continue;

                    if (string.IsNullOrWhiteSpace(consumeResult.Message.Value))
                    {
                        await _console.Error.WriteLineAsync("Value is empty. Reading next one...");
                        continue;
                    }

                    var json = JObject.Parse(consumeResult.Message.Value);
                }
            }
            finally
            {
                ExtractorHelper.Unsubscribe(topicConsumer, _console);
            }
            return 1; // ok
        }
    }
}