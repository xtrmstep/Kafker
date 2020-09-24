using Confluent.Kafka;
using Kafker.Configurations;

namespace Kafker.Kafka
{
    public interface IConsumerFactory
    {
        RecordsConsumer Create(KafkaTopicConfiguration cfg);
    }
}