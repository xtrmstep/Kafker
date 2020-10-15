
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafker.Helpers
{
    public interface IFileHandler
    {
        Task SaveToFileAsync(FileInfo destinationCsvFile, Timestamp messageTimeStamp, string messageValue);
        Task<IEnumerable<string>> LoadFromFileAsync(string sourceFile);
    }
}