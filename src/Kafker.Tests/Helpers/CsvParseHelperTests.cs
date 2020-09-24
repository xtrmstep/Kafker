using System;
using FluentAssertions;
using Kafker.Configurations;
using Kafker.Helpers;
using Xunit;

namespace Kafker.Tests.Helpers
{
    public class CsvParseHelperTests
    {
        [Theory]
        [InlineData(@"""simple"",""strings""", new[]{"simple","strings"})]
        [InlineData(@"""row with commas"",""string,with,comma""", new[]{"row with commas","string,with,comma"})]
        [InlineData(@"""row with a number"",3.0", new[]{"row with a number","3.0"})]
        public void Should_create_filename_with_datetime_stamp(string input, string[] expected)
        {
            var actual = CsvParseHelper.GetValues(input, ",", "\"");
            actual.Should().BeEquivalentTo(expected);
        }
    }
}