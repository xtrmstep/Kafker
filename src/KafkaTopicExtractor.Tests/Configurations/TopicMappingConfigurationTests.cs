using System;
using System.IO;
using FluentAssertions;
using KafkaTopicExtractor.Configurations;
using Newtonsoft.Json.Linq;
using Xunit;

namespace KafkaTopicExtractor.Tests.Configurations
{
    public class TopicMappingConfigurationTests
    {
        private readonly string _sampleJsonFile = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, ".\\sample.json"));
        private readonly JObject _jsonObject;

        public TopicMappingConfigurationTests()
        {
            _jsonObject = JObject.Parse(File.ReadAllText(_sampleJsonFile));
        }
        
        [Fact]
        public void When_loaded_should_have_corresponding_number_of_keys()
        {
            
            var actual = new TopicMappingConfiguration(_jsonObject);

            actual.Mapping.Count.Should().Be(7);
        }
        
        [Fact]
        public void When_loaded_should_have_correct_key_names()
        {
            
            var actual = new TopicMappingConfiguration(_jsonObject);
            var expected = new[]
            {
                "Name",
                "Color",
                "Attributes[0].Name",
                "Attributes[0].Value",
                "Attributes[1].Name",
                "Attributes[1].Value[0].Type",
                "Attributes[1].Value[0].Length"
            };

            actual.Mapping.Keys.Should().BeEquivalentTo(expected);
        }
    }
}