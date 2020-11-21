﻿using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Emitters
{
    public class SimpleEventsEventsEmitter : IEventsEmitter
    {
        private readonly IConsole _console;
        private readonly IProducerFactory _producerFactory;
        private readonly KafkerSettings _settings;
        protected int ProducedEvents = 0;

        public SimpleEventsEventsEmitter(IConsole console, IProducerFactory producerFactory,KafkerSettings settings)
        {
            _console = console;
            _producerFactory = producerFactory;
            _settings = settings;
        }
        
        public async Task<int> EmitEvents(CancellationToken cancellationToken, KafkaTopicConfiguration cfg, string fileName)
        {
            Func<Task> writeProducedEvents = async () => await _console.Out.WriteLineAsync($"\r\nEmitted {ProducedEvents} events");

            _console.CancelKeyPress += (sender, args) => writeProducedEvents().GetAwaiter().GetResult();
            await _console.Out.WriteLineAsync("Press CTRL+C to interrupt the read operation");
            
            using var topicProducer = _producerFactory.Create(cfg);
            try
            {
                await PrivateEmitEvents(cancellationToken, fileName, topicProducer, cfg.EventsToRead);
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await _console.Error.WriteLineAsync(e.Message);
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
            finally
            {
                await writeProducedEvents();
            }            
        }

        protected virtual async Task PrivateEmitEvents(CancellationToken cancellationToken, string fileName, RecordsProducer topicProducer, uint eventsToRead)
        {
            using var reader = new StreamReader(fileName);
            string line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (cancellationToken.IsCancellationRequested) break;

                var pair = line.Split("|");
                var jsonText = pair[1].Substring(1, pair[1].Length - 2);
                await topicProducer.ProduceAsync(jsonText);
                ProducedEvents++;
                
                // inform about progress
                if (eventsToRead == 0)
                    await _console.Out.WriteAsync($"\remitting {ProducedEvents}...");
                else
                    await _console.Out.WriteAsync($"\remitting {ProducedEvents / (double)eventsToRead * 100:f2}% [{ProducedEvents}/{eventsToRead}]...");
                
                // exit if read required number of events
                if (eventsToRead != 0 && ProducedEvents >= eventsToRead) break;
            }
        }
    }
}