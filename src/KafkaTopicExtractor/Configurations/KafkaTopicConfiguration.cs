namespace KafkaTopicExtractor.Configurations
{
    public class KafkaTopicConfiguration
    {
        public string[] Brokers { get; set; } = new string[0];
        public string Topic { get; set; }
        public uint EventsToRead { get; set; } = 0; // infinite
        public OffsetKind OffsetKind { get; set; } = OffsetKind.Latest;
    }

    public enum OffsetKind
    {
        Earliest,
        Latest
    }
}