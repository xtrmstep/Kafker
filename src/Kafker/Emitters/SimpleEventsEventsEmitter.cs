using System;
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
            using var topicProducer = _producerFactory.Create(cfg);
            try
            {
                await PrivateEmitEvents(cancellationToken, fileName, topicProducer);
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await _console.Error.WriteLineAsync(e.Message);
                return await Task.FromResult(Constants.RESULT_CODE_ERROR).ConfigureAwait(false);
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\nProduced {ProducedEvents} events");
            }            
        }

        protected virtual async Task PrivateEmitEvents(CancellationToken cancellationToken, string fileName, RecordsProducer topicProducer)
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
            }
        }
    }
}