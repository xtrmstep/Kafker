using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Options;

namespace Kafker.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;

        public EmitCommand(IConsole console, IOptions<KafkerSettings> settings)
        {
            _console = console;
            _settings = settings.Value;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var mapping = await ExtractorHelper.ReadMappingConfigurationAsync(topic, _settings, _console);

            using var topicProducer = ExtractorHelper.CreateKafkaTopicProducer(cfg, _console);

            var producedEvents = 0;
            try
            {
                var recordsBuffer = new RecordsBuffer();
                await recordsBuffer.LoadFromFileAsync(fileName);
                var records = await recordsBuffer.GetJsonRecordsAsync(mapping);
                foreach (var record in records)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await ExtractorHelper.ProduceAsync(topicProducer, cfg, record);
                    producedEvents++;
                }
            }
            finally
            {
                await _console.Out.WriteLineAsync($"Produced {producedEvents} events"); 
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}