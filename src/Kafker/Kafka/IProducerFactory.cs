using Confluent.Kafka;
using Kafker.Configurations;

namespace Kafker.Kafka
{
    public interface IProducerFactory
    {
        RecordsProducer Create(KafkaTopicConfiguration cfg);
    }
}