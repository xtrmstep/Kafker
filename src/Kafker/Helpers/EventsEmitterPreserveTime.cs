using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class EventsEmitterPreserveTime : SimpleEventsEventsEmitter
    {
        static volatile bool startEvent = false;
        private readonly IConsole _console;
        private static IProducerFactory _producerFactory;
        private readonly KafkerSettings _settings;
        private static int _producedEvents = 0;

        public EventsEmitterPreserveTime(IConsole console, IProducerFactory producerFactory,
            KafkerSettings settings) : base(console, producerFactory, settings)
        {
            _console = console;
            _producerFactory = producerFactory;
            _settings = settings;
        }

        public override async Task<int> EmitEvents(CancellationToken cancellationToken, KafkaTopicConfiguration cfg, string fileName)
        {
            var topicProducer = _producerFactory.Create(cfg);

            try
            {
                var mappedEventsWithTimeStamp = await MapEventsWithTimeStamp(fileName);
                var tasks = mappedEventsWithTimeStamp.Select(x => ScheduleEventsSending(x.Key, x.Value, topicProducer, cancellationToken)).ToArray();

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
                await _console.Out.WriteLineAsync($"\r\nProduced {_producedEvents} events");
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }

        private async Task<List<Tuple<long, string>>> LoadAndSplitSnapshotData(string fileName)
        {
            var allLines = await File.ReadAllLinesAsync(fileName);
            var initialSnapshot = allLines.Select(item => item.Split("|")).Select(pair => new Tuple<long, string>(long.Parse(pair[0].Replace("\"", "")), pair[1])).ToList();

            return initialSnapshot;
        }

        private async Task<Dictionary<long, List<string>>> MapEventsWithTimeStamp(string fileName)
        {
            var listOfSnapshotTuples = await LoadAndSplitSnapshotData(fileName);
            var timeStampWithEvents = new List<Tuple<long, string>>();
            var eventsMappedByTime = new Dictionary<long, List<string>>();

            var smallest = listOfSnapshotTuples.Select(x => x.Item1).Min();

            foreach (var (timeStamp, itemToSend) in listOfSnapshotTuples)
            {
                timeStampWithEvents.Add(new Tuple<long, string>(timeStamp - smallest, itemToSend));
            }

            foreach (var (timeStamp, itemToSend) in timeStampWithEvents)
            {
                if (!eventsMappedByTime.ContainsKey(timeStamp))
                {
                    eventsMappedByTime.Add(timeStamp, new List<string>());
                }

                eventsMappedByTime[timeStamp].Add(itemToSend);
            }

            return eventsMappedByTime;
        }

        private static async Task ScheduleEventsSending(long key, List<string> value, RecordsProducer producer, CancellationToken cancellationToken)
        {
            await Task.Yield();

            while (!startEvent)
            {
                await Task.Delay(1, cancellationToken);
                if (cancellationToken.IsCancellationRequested) return;
            }

            await Task.Delay((int) key, cancellationToken);
            if (!cancellationToken.IsCancellationRequested)
            {
                EmitEventsOnTime(value, producer);
            }
        }

        private static void EmitEventsOnTime(List<string> list, RecordsProducer producer)
        {
            foreach (var item in list)
            {
                producer.ProduceAsync(item).GetAwaiter().GetResult();
                _producedEvents++;
            }
        }
    }
}