using System;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;
        private readonly IProducerFactory _producerFactory;
        private readonly IEmitter _emitter;

        public EmitCommand(IConsole console, KafkerSettings settings, IProducerFactory producerFactory, IEmitter emitter)
        {
            _console = console;
            _settings = settings;
            _producerFactory = producerFactory;
            _emitter = emitter;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName )
        {
            try
            {
                await _emitter.EmitEvents(cancellationToken,fileName,topic);
               
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
    }
}
