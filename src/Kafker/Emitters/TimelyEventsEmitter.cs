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
    public class TimelyEventsEmitter : SimpleEventsEmitter
    {
        static volatile bool _startEvent = false;

        public TimelyEventsEmitter(IConsole console, IProducerFactory producerFactory,
            KafkerSettings settings) : base(console, producerFactory, settings)
        {
        }        

        private static async Task<List<Tuple<long, string>>> LoadEventsFromFileAsync(string fileName, uint eventsToRead)
        {
            var lines = new List<string>();
            if (eventsToRead == 0)
                lines.AddRange(await File.ReadAllLinesAsync(fileName));
            else
                lines.AddRange(await ReadLinesFromFileAsync(fileName, eventsToRead));
            
            var tuples = lines.Select(item => item.Split("|")).Select(pair => new Tuple<long, string>(long.Parse(pair[0]), pair[1])).ToList();

            return tuples;
        }

        private static async Task<string[]> ReadLinesFromFileAsync(string fileName, uint eventsToRead)
        {
            var lines = new List<string>();
            var idx = 0;
            var reader = File.OpenText(fileName);
            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                idx++;
                lines.Add(line);

                if (eventsToRead != 0 && idx >= eventsToRead) break;
            }

            return lines.ToArray();
        }

        private Dictionary<long, List<string>> GroupEventsByTime(IList<Tuple<long, string>> listOfSnapshotTuples)
        {
            var minTime = listOfSnapshotTuples.Select(x => x.Item1).Min();
            var eventsWithTime = listOfSnapshotTuples.Select(tuple => new Tuple<long, string>(tuple.Item1 - minTime, tuple.Item2)).ToList();
            var eventsGroupedByTime = eventsWithTime.GroupBy(e => e.Item1, e => e.Item2).ToDictionary(r => r.Key, r => r.ToList());
            return eventsGroupedByTime;
        }

        private async Task ScheduleEventsSending(long key, List<string> value, RecordsProducer producer, CancellationToken cancellationToken, Func<Task> reportProgress)
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
                EmitEventsOnTime(value, producer, cancellationToken, reportProgress);
            }
        }

        private void EmitEventsOnTime(IList<string> list, RecordsProducer producer, CancellationToken cancellationToken, Func<Task> reportProgress)
        {
            foreach (var item in list)
            {
                if (cancellationToken.IsCancellationRequested) return;
                producer.ProduceAsync(item).GetAwaiter().GetResult();
                
                Interlocked.Increment(ref ProducedEvents);
                reportProgress();                
            }
        }

        /// <inheritdoc />
        protected override async Task PrivateEmitEvents(CancellationToken cancellationToken, string fileName, RecordsProducer topicProducer, uint limitEventsNumber)
        {
            var events = await LoadEventsFromFileAsync(fileName, limitEventsNumber);
           EventsToEmit = (uint) events.Count;

            var eventsByTime = GroupEventsByTime(events);
            var tasks = eventsByTime.Select(x => ScheduleEventsSending(x.Key, x.Value, topicProducer, cancellationToken, ReportProgress)).ToArray();

            _startEvent = true;
            Task.WaitAll(tasks, cancellationToken);
        }
    }
}