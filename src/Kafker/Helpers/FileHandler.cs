using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class FileHandler : IFileHandler
    {
        private readonly IConsole _console;

        public FileHandler(IConsole console)
        {
            _console = console;
        }

        public async Task SaveToFileAsync(FileInfo destinationCsvFile, Timestamp messageTimeStamp, string messageValue)
        {
            await Task.Yield();
            await using var fileStream = new FileStream(destinationCsvFile.FullName, FileMode.Append, FileAccess.Write);
            await using var streamWriter = new StreamWriter(fileStream);
            await streamWriter.WriteLineAsync($"\"{messageTimeStamp.UnixTimestampMs}\"|\"{messageValue}\"");
        }

        public async Task<IEnumerable<string>> LoadFromFileAsync(string sourceFile)
        {
            await Task.Yield();
            var idx = 0;
            var lines = new List<string>();
            using (var reader = new StreamReader(sourceFile))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    lines.Add(line);
                }

                foreach (var item in lines)
                {
                    var pair = item.Split("|");
                    var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                    var record = pair[1].Substring(1, pair[1].Length - 2);
                }
            }

            await _console.Out.WriteAsync($"\rloaded {idx++}...");

            return lines;
        }
    }
}