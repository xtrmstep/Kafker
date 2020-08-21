using System.Collections.Generic;

namespace KafkaTopicExtractor.Configurations
{
    public class TopicMappingConfiguration
    {
        public IDictionary<string, string> Mapping { get; set; } = new Dictionary<string, string>();
    }
}