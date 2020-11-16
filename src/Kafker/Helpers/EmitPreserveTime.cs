using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Kafker.Commands;
using Kafker.Configurations;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class EmitPreserveTime : Emit
    {
        static volatile bool startEvent = false;
        private readonly IConsole _console;
        private static IProducerFactory _producerFactory;
        private readonly KafkerSettings _settings;

        public EmitPreserveTime(IConsole console, IProducerFactory producerFactory,
            KafkerSettings settings) : base(console, producerFactory, settings)
        {
            _console = console;
            _producerFactory = producerFactory;
            _settings = settings;
        }

        public override async Task<int> EmitEvents(CancellationToken cancellationToken, string fileName, string topic)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);
            var producedEvents = 0;

            try
            {
                var getDictionary = await CreatePreserveTime(fileName);
                var tasks = getDictionary.Select(x => SetTimer(x.Key, x.Value, cfg)).ToArray();

                startEvent = true;
                Task.WaitAll(tasks);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
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

        private static async Task SetTimer(long key, List<string> value, KafkaTopicConfiguration config)
        {
            await Task.Yield();
            var stopEvent = new AutoResetEvent(false);

            var data = new PreserveTime
            {
                ItemsToSend = value,
                ProducerConfig = config,
                Waiter = stopEvent
            };
            while (!startEvent)
            {
                Task.Yield();
            }

            var timer = new Timer(EmitEventsOnTime, data, key, Timeout.Infinite);
            stopEvent.WaitOne();
            //producedEvents++;
        }

        private static void EmitEventsOnTime(object state)
        {
            if (state != null)
            {
                var preserver = (PreserveTime) state;

                using var topicProducer = _producerFactory.Create(preserver.ProducerConfig);
                foreach (var item in preserver.ItemsToSend)
                {
                    topicProducer.ProduceAsync(item).GetAwaiter().GetResult();
                }

                preserver.Waiter.Set();
            }
        }
    }

    class PreserveTime
    {
        public List<string> ItemsToSend { get; set; }
        public AutoResetEvent Waiter { get; set; }
        public AutoResetEvent Start { get; set; }
        public KafkaTopicConfiguration ProducerConfig { get; set; }
    }
}