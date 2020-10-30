using System;
using Confluent.Kafka;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Kafka
{
    public class ConsumerFactory : IConsumerFactory
    {
        private readonly IConsole _console;

        public ConsumerFactory(IConsole console)
        {
            _console = console;
        }
        
        /// <inheritdoc />
        public RecordsConsumer Create(KafkaTopicConfiguration config)
        {
            var rc = new RecordsConsumer(_console, config);
            rc.Subscribe();
            return rc;
        }        
    }
}