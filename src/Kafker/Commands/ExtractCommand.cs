using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kafker.Commands
{
    public class ExtractCommand : IExtractCommand
    {
        private readonly IConsole _console;
        private readonly IFileTagProvider _fileTagProvider;
        private readonly IConsumerFactory _consumerFactory;
        private readonly KafkerSettings _settings;

        public ExtractCommand(IConsole console, IFileTagProvider fileTagProvider, KafkerSettings settings,
            IConsumerFactory consumerFactory)
        {
            _console = console;
            _fileTagProvider = fileTagProvider;
            _consumerFactory = consumerFactory;
            _settings = settings;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, KafkaTopicConfiguration configuration)
        {
            var destinationCsvFile = GetDestinationCsvFilename(_settings.ConfigurationFolder, _settings, _fileTagProvider);
            var totalNumberOfConsumedEvents = 0;
            using var topicConsumer = _consumerFactory.Create(configuration);
            await using var fileStream = new FileStream(destinationCsvFile.FullName, FileMode.Append, FileAccess.Write);
            await using var streamWriter = new StreamWriter(fileStream);
            
            try
            {
                var numberOfReadEvents = 0;
                var totalEventsToRead = configuration.EventsToRead; // 0 - infinite
                totalNumberOfConsumedEvents = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = topicConsumer.Consume(cancellationToken);
                    if (consumeResult.IsPartitionEOF) break;
                    totalNumberOfConsumedEvents++;

                    if (string.IsNullOrWhiteSpace(consumeResult.Message.Value))
                    {
                        await _console.Error.WriteLineAsync("Value is empty or not read. Reading next one...");
                        continue;
                    }

                    numberOfReadEvents++;
                    var message = JObject.Parse(consumeResult.Message.Value).ToString(Formatting.None);
                    await streamWriter.WriteLineAsync($"{consumeResult.Message.Timestamp.UnixTimestampMs}|{message}");

                    if (totalEventsToRead > 0)
                        await _console.Out.WriteAsync($"\rloaded {numberOfReadEvents}/{totalEventsToRead}...");
                    else
                        await _console.Out.WriteAsync($"\rloaded {numberOfReadEvents}...");

                    // check if we need to stop reading events
                    if (totalEventsToRead > 0 && numberOfReadEvents >= totalEventsToRead)
                        break;
                }
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (Exception err)
            {
                await _console.Error.WriteLineAsync($"\n\rError: {err.Message}");
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\n\rConsumed {totalNumberOfConsumedEvents} events");
            }
        }

        internal static FileInfo GetDestinationCsvFilename(string topic, KafkerSettings setting,
            IFileTagProvider fileTagProvider)
        {
            var tag = fileTagProvider.GetTag();
            var filePath = Path.Combine(setting.Destination, $"{topic}_{tag}.dat");
            var fileInfo = new FileInfo(filePath);
            return fileInfo;
        }
    }
}