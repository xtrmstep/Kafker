using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Kafka
{
    public class ProducerFactory : IProducerFactory
    {
        private readonly IConsole _console;

        public ProducerFactory(IConsole console)
        {
            _console = console;
        }
        
        /// <inheritdoc />
        public RecordsProducer Create(KafkaTopicConfiguration cfg)
        {
            var rp = new RecordsProducer(_console, cfg);
            return rp;
        }
    }
}