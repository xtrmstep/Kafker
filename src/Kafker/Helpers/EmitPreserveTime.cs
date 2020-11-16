using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Commands;
using Kafker.Configurations;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class EmitPreserveTime : Emit
    {
        private readonly IConsole _console;
        private readonly IProducerFactory _producerFactory;
        private readonly KafkerSettings _settings;

        public EmitPreserveTime(IConsole console,IProducerFactory producerFactory,
            KafkerSettings settings) : base(console,producerFactory,settings)
        {
            _console = console;
            _producerFactory = producerFactory;
            _settings = settings;
        }
        
        public override async Task<int> EmitEvents(CancellationToken cancellationToken,string fileName,string topic)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings,_console);
            var timerList = new List<Timer>();
            var producedEvents = 0;
            
            try
            {
                var getDictionary = await CreatePreserveTime(fileName);
                using var stateObject = new PreserveTime();
                var waitHandles = new List<WaitHandle>();
                foreach (var item in getDictionary)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    stateObject.ItemsToSend = item.Value;
                    stateObject.ProducerConfig = cfg;
                    stateObject.ResetEvent = new AutoResetEvent(false);
                    waitHandles.Add(stateObject.ResetEvent);
                    var delay = item.Key;
                    timerList.Add(new Timer(EmitEventsOnTime, stateObject, delay, Timeout.Infinite));
                    producedEvents++;
                }

                WaitHandle.WaitAll(waitHandles.ToArray());
                
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                foreach (var item in timerList)
                {
                    await item.DisposeAsync();
                }

                await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
            }
            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }
        
        private async Task<Dictionary<long, List<string>>> CreatePreserveTime(string fileName)
        {
            var allLines = await File.ReadAllLinesAsync(fileName);
            var timeStampWithEvents = new List<Tuple<long, string>>();
            var eventsToEmit = new Dictionary<long, List<string>>();
            var initialSnapshot = allLines.Select(item => item.Split("|")).Select(pair => new Tuple<long, string>(long.Parse(pair[0].Replace("\"", "")), pair[1])).ToList();
            var smallest = initialSnapshot.Select(x => x.Item1).Min();

            foreach (var (timeStamp, itemToSend) in initialSnapshot)
            {
                timeStampWithEvents.Add(new Tuple<long, string>(timeStamp - smallest, itemToSend));
            }

            foreach (var (timeStamp, itemToSend) in timeStampWithEvents)
            {
                if (!eventsToEmit.ContainsKey(timeStamp))
                {
                    eventsToEmit.Add(timeStamp, new List<string>());
                }

                eventsToEmit[timeStamp].Add(itemToSend);
            }

            return eventsToEmit;
        }
        
        private void EmitEventsOnTime(object state)
        {
            if (state != null)
            {
                var preserver = (PreserveTime) state;
                
                using var topicProducer = _producerFactory.Create(preserver.ProducerConfig);
                foreach (var item in preserver.ItemsToSend)
                {
                    topicProducer.ProduceAsync(item).GetAwaiter().GetResult();
                }

                preserver.ResetEvent.Set();
            }
        }
    }
    
    class PreserveTime : IDisposable
    {
        public List<string> ItemsToSend { get; set; }
        public AutoResetEvent ResetEvent { get; set; }
        public KafkaTopicConfiguration ProducerConfig { get; set; }

        public void Dispose()
        {
            ResetEvent?.Dispose();
        }
    }
}