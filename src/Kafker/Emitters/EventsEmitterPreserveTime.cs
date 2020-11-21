using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Emitters
{
    public class EventsEmitterPreserveTime : SimpleEventsEventsEmitter
    {
        static volatile bool _startEvent = false;
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
                var events = await LoadEventsFromFileAsync(fileName);
                var eventsByTime = GroupEventsByTime(events);
                var tasks = eventsByTime.Select(x => ScheduleEventsSending(x.Key, x.Value, topicProducer, cancellationToken)).ToArray();

                _startEvent = true;
                Task.WaitAll(tasks, cancellationToken);
            }
            catch (Exception e)
            {
                await _console.Error.WriteLineAsync(e.Message);
                throw;
            }
            finally
            {
                await _console.Out.WriteLineAsync($"\r\nProduced {_producedEvents} events");
            }

            return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false); // ok
        }

        private static async Task<List<Tuple<long, string>>> LoadEventsFromFileAsync(string fileName)
        {
            var allLines = await File.ReadAllLinesAsync(fileName);
            var initialSnapshot = allLines.Select(item => item.Split("|")).Select(pair => new Tuple<long, string>(long.Parse(pair[0]), pair[1])).ToList();

            return initialSnapshot;
        }

        private Dictionary<long, List<string>> GroupEventsByTime(IList<Tuple<long, string>> listOfSnapshotTuples)
        {
            var minTime = listOfSnapshotTuples.Select(x => x.Item1).Min();
            var eventsWithTime = listOfSnapshotTuples.Select(tuple => new Tuple<long, string>(tuple.Item1 - minTime, tuple.Item2)).ToList();
            var eventsGroupedByTime = eventsWithTime.GroupBy(e => e.Item1, e => e.Item2).ToDictionary(r => r.Key, r => r.ToList());
            return eventsGroupedByTime;
        }

        private static async Task ScheduleEventsSending(long key, List<string> value, RecordsProducer producer, CancellationToken cancellationToken)
        {
            await Task.Yield();

            while (!_startEvent)
            {
                await Task.Delay(1, cancellationToken);
                if (cancellationToken.IsCancellationRequested) return;
            }

            await Task.Delay((int) key, cancellationToken);
            if (!cancellationToken.IsCancellationRequested)
            {
                EmitEventsOnTime(value, producer, cancellationToken);
            }
        }

        private static void EmitEventsOnTime(List<string> list, RecordsProducer producer, CancellationToken cancellationToken)
        {
            foreach (var item in list)
            {
                if (cancellationToken.IsCancellationRequested) return;
                
                producer.ProduceAsync(item).GetAwaiter().GetResult();
                _producedEvents++;
            }
        }
    }
}