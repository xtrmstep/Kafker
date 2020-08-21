using System;

namespace KafkaTopicExtractor.Helpers
{
    public class FileTagProvider : IFileTagProvider
    {
        private readonly DateTimeOffset _dateTimeOffset;

        public FileTagProvider()
        {
            _dateTimeOffset = DateTimeOffset.Now;
        }

        public FileTagProvider(DateTimeOffset dateTimeOffset)
        {
            _dateTimeOffset = dateTimeOffset;
        }

        public string GetTag()
        {
            return $"{_dateTimeOffset:yyyyMMdd}_{_dateTimeOffset:hhmmss}";
        }
    }
}