using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Kafker.Configurations
{
    public class KafkaTopicConfiguration
    {
        public string[] Brokers { get; set; } = new string[0];
        public string Topic { get; set; }
        public uint EventsToRead { get; set; } = 0; // infinite

        [JsonConverter(typeof(StringEnumConverter))]
        public OffsetKind OffsetKind { get; set; } = OffsetKind.Latest;
    }

    public enum OffsetKind
    {
        Earliest,
        Latest
    }
}