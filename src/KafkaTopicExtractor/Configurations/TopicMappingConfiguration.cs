using System.Collections.Generic;
using JsonFlatten;
using Newtonsoft.Json.Linq;

namespace KafkaTopicExtractor.Configurations
{
    public class TopicMappingConfiguration
    {
        public TopicMappingConfiguration()
        {
        }

        public TopicMappingConfiguration(JObject json)
        {
            var dic = json.Flatten();
            Mapping = new Dictionary<string, string>();
            foreach (var dicKey in dic.Keys)
            {
                Mapping.Add(dicKey, dicKey);
            }
        }

        public IDictionary<string, string> Mapping { get; set; } = new Dictionary<string, string>();
    }
}