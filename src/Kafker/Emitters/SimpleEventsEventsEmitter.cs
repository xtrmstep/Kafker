using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class SimpleEventsEventsEmitter : IEventsEmitter
    {
        private readonly IConsole _console;
        private readonly IProducerFactory _producerFactory;
        private readonly KafkerSettings _settings;

        public SimpleEventsEventsEmitter(IConsole console, IProducerFactory producerFactory,KafkerSettings settings)
        {
            _console = console;
            _producerFactory = producerFactory;
            _settings = settings;
        }
        
        public virtual async Task<int> EmitEvents(CancellationToken cancellationToken, KafkaTopicConfiguration cfg, string filename)
        {
            var producedEvents = 0;
            using var topicProducer = _producerFactory.Create(cfg);

            using var reader = new StreamReader(filename);
            try
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    var pair = line.Split("|");
                    var jsonText = pair[1].Substring(1, pair[1].Length - 2);
                    await topicProducer.ProduceAsync(jsonText);
                    producedEvents++;
                }
            }
            catch (Exception e)
            {
                await _console.Out.WriteLineAsync(e.Message);
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
            }
            
            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false); // ok
        }
    }
}