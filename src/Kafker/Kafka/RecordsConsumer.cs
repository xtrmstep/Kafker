using System;
using System.Threading;
using Confluent.Kafka;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Kafka
{
    public class RecordsConsumer : IDisposable
    {
        private readonly KafkaTopicConfiguration _config;
        private readonly IConsole _console;
        private readonly IConsumer<Ignore, string> _consumer;
        private bool _isUnsubscribed;

        public RecordsConsumer(IConsole console, KafkaTopicConfiguration config)
        {
            _console = console;
            _config = config;

            var dt = DateTimeOffset.Now;
            var consumerGroupTag = $"{dt:yyyyMMdd}_{dt:hhmmss}";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = string.Join(',', config.Brokers),
                GroupId = $"kafka_topic_extractor_{consumerGroupTag}",
                EnableAutoCommit = true,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = config.OffsetKind == OffsetKind.Earliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
                EnablePartitionEof = true
            };
            var consumerBuilder = new ConsumerBuilder<Ignore, string>(consumerConfig);
            _consumer = consumerBuilder.Build();

            _console.WriteLine($"Created a consumer: {consumerConfig.GroupId}");
            _console.WriteLine($"    brokers: {consumerConfig.BootstrapServers}");
            _console.WriteLine($"    autoOffsetReset: {consumerConfig.AutoOffsetReset}");
            _console.WriteLine($"    topic: {config.Topic}");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (!_isUnsubscribed) Unsubscribe();
        }

        public void Unsubscribe()
        {
            _consumer.Unsubscribe();
            _isUnsubscribed = true;
            _console.WriteLine("Consumer unsubscribed");
        }

        public void Subscribe()
        {
            _consumer.Subscribe(_config.Topic);
        }

        public ConsumeResult<Ignore, string> Consume(in CancellationToken cancellationToken)
        {
            return _consumer.Consume(cancellationToken);
        }
    }
}