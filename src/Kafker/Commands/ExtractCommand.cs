using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace Kafker.Commands
{
    public class ExtractCommand : IExtractCommand
    {
        private readonly IConsole _console;
        private readonly IFileTagProvider _fileTagProvider;
        private readonly KafkerSettings _settings;

        public ExtractCommand(IConsole console, IFileTagProvider fileTagProvider, IOptions<KafkerSettings> settings)
        {
            _console = console;
            _fileTagProvider = fileTagProvider;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string map)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var mapping = await ExtractorHelper.ReadMappingConfigurationAsync(map ?? topic, _settings, _console);

            using var topicConsumer = ExtractorHelper.CreateKafkaTopicConsumer(cfg, _console);
            var destinationCsvFile = GetDestinationCsvFilename(topic, _settings, _fileTagProvider);
            
            var consumedEventsInTotal = 0;
            try
            {
                var eventNumber = 0;
                var totalEventsToRead = cfg.EventsToRead; // 0 - infinite
                consumedEventsInTotal = 0;
                var recordsBuffer = new RecordsBuffer();
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = topicConsumer.Consume(cancellationToken);
                    consumedEventsInTotal++;
                    if (consumeResult.IsPartitionEOF) continue;

                    if (string.IsNullOrWhiteSpace(consumeResult.Message.Value))
                    {
                        await _console.Error.WriteLineAsync("Value is empty or not read. Reading next one...");
                        continue;
                    }

                    eventNumber++;

                    var json = JObject.Parse(consumeResult.Message.Value);
                    recordsBuffer.Add(json);                    

                    if (totalEventsToRead > 0)
                        await _console.Out.WriteLineAsync($"  processed {eventNumber}/{totalEventsToRead}");
                    else
                        await _console.Out.WriteLineAsync($"  processed {eventNumber}");

                    // check if we need to stop reading events
                    if (totalEventsToRead > 0 && eventNumber >= totalEventsToRead)
                        break;
                }
                await recordsBuffer.SaveToFileAsync(destinationCsvFile, mapping);
            }
            finally
            {
                await _console.Out.WriteLineAsync($"Consumed {consumedEventsInTotal} events");                
                ExtractorHelper.Unsubscribe(topicConsumer, _console);
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }

        internal static FileInfo GetDestinationCsvFilename(string topic, KafkerSettings setting, IFileTagProvider fileTagProvider)
        {
            var tag = fileTagProvider.GetTag();
            var filePath = Path.Combine(setting.Destination, $"{topic}_{tag}.csv");
            var fileInfo = new FileInfo(filePath);
            return fileInfo;
        }
    }
}