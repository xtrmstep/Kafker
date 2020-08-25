using System;
using FluentAssertions;
using KafkaTopicExtractor.Helpers;
using Xunit;

namespace KafkaTopicExtractor.Tests
{
    public class FileTagProviderTests
    {
        [Fact]
        public void Should_datetime_tag()
        {
            var actual = new FileTagProvider(new DateTimeOffset(2020, 1, 2, 5, 34, 43, TimeSpan.Zero)).GetTag();
            actual.Should().Be("20200102_053443");
        }
    }
}