﻿using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

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

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string map)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var destinationCsvFile = GetDestinationCsvFilename(topic, _settings, _fileTagProvider);

            var totalNumberOfConsumedEvents = 0;
            using var topicConsumer = _consumerFactory.Create(cfg);
            await using var fileStream = new FileStream(destinationCsvFile.FullName, FileMode.Append, FileAccess.Write);
            await using var streamWriter = new StreamWriter(fileStream);
            try
            {
                var numberOfReadEvents = 0;
                var totalEventsToRead = cfg.EventsToRead; // 0 - infinite
                totalNumberOfConsumedEvents = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = topicConsumer.Consume(cancellationToken);
                    totalNumberOfConsumedEvents++;
                    if (consumeResult.IsPartitionEOF) break;

                    if (string.IsNullOrWhiteSpace(consumeResult.Message.Value))
                    {
                        await _console.Error.WriteLineAsync("Value is empty or not read. Reading next one...");
                        continue;
                    }

                    numberOfReadEvents++;
                    await streamWriter.WriteLineAsync($"\"{consumeResult.Message.Timestamp.UnixTimestampMs}\"|\"{consumeResult.Message.Value}\"");
                   

                    if (totalEventsToRead > 0)
                        await _console.Out.WriteAsync($"\rloaded {numberOfReadEvents}/{totalEventsToRead}...");
                    else
                        await _console.Out.WriteAsync($"\rloaded {numberOfReadEvents}...");

                    // check if we need to stop reading events
                    if (totalEventsToRead > 0 && numberOfReadEvents >= totalEventsToRead)
                        break;
                }
            }
                    
            finally
            {
                await _console.Out.WriteLineAsync($"\n\rConsumed {totalNumberOfConsumedEvents} events");
            }
            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }

        internal static FileInfo GetDestinationCsvFilename(string topic, KafkerSettings setting,
            IFileTagProvider fileTagProvider)
        {
            var tag = fileTagProvider.GetTag();
            var filePath = Path.Combine(setting.Destination, $"{topic}_{tag}.csv");
            var fileInfo = new FileInfo(filePath);
            return fileInfo;
        }
    }
}