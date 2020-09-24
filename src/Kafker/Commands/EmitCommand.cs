﻿using System;
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
            var flattenBuffer = ExtractorHelper.CreateFlattenBuffer();
            await flattenBuffer.LoadFromFileAsync(sourceCsvFile);

            var producedEvents = 0;
            try
            {
                producedEvents = 0;
                var records = flattenBuffer.GetJsonRecords();
                foreach (var record in records)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await ExtractorHelper.ProduceAsync(topicProducer, cfg, record);

                }
            }
            catch (Exception er)
            {
                throw;
            }
            finally
            {
                await _console.Out.WriteLineAsync($"Produced {producedEvents} events"); 
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}