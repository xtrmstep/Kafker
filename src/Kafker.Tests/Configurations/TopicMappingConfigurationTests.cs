using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using JsonFlatten;
using Kafker.Configurations;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Kafker.Tests.Configurations
{
    public class TopicMappingConfigurationTests
    {
        private readonly JObject _jsonObject;

        private readonly string _sampleJsonFile =
            Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, ".\\sample.json"));

        public TopicMappingConfigurationTests()
        {
            _jsonObject = JObject.Parse(File.ReadAllText(_sampleJsonFile));
        }

        [Fact]
        public void When_loaded_should_have_corresponding_number_of_keys()
        {
            var actual = new KafkaTopicConfiguration();
            var dic = _jsonObject.Flatten();
            actual.Mapping = new Dictionary<string, string>();
            foreach (var dicKey in dic.Keys)
            {
                actual.Mapping.Add(dicKey, dicKey);
            }
            actual.Mapping.Count.Should().Be(7);
        }

        [Fact]
        public void When_loaded_should_have_correct_key_names()
        {
            var actual = new KafkaTopicConfiguration();
            var dic = _jsonObject.Flatten();
            actual.Mapping = new Dictionary<string, string>();
            foreach (var dicKey in dic.Keys)
            {
                actual.Mapping.Add(dicKey, dicKey);
            }
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