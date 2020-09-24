using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Kafka
{
    public class RecordsProducer : IDisposable
    {
        private readonly IConsole _console;
        private readonly KafkaTopicConfiguration _config;
        private IProducer<string, string> _producer;

        public RecordsProducer(IConsole console, KafkaTopicConfiguration config)
        {
            _console = console;
            _config = config;
            
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = string.Join(',', config.Brokers)
            };
            var producerBuilder = new ProducerBuilder<string, string>(producerConfig);
            _producer = producerBuilder.Build();
            
            console.WriteLine($"Created a producer:");
            console.WriteLine($"    brokers: {producerConfig.BootstrapServers}");
            console.WriteLine($"    topic: {config.Topic}");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _producer.Flush();
            _producer.Dispose();
            _producer = null;
        }

        public async Task ProduceAsync(string record)
        {
            var message = new Message<string, string>
            {
                Key = string.Empty,
                Value = record
            };
            await _producer.ProduceAsync(_config.Topic, message);
        }
    }
}