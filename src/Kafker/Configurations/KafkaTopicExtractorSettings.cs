using System.Dynamic;

namespace Kafker.Configurations
{
    public class KafkerSettings
    {
        public string Destination { get; set; } = string.Empty;
        public string ConfigurationFolder { get; set; }
        
        public string[]  Brokers { get; set; }
        
    }
    
}